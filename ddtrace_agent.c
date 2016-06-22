/*-
 * Copyright (c) 2016 (holder)
 * All rights reserved.
 *
 * This software was developed by BAE Systems, the University of Cambridge
 * Computer Laboratory, and Memorial University under DARPA/AFRL contract
 * FA8650-15-C-7558 ("CADETS"), as part of the DARPA Transparent Computing
 * (TC) research program.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 */

#include <sys/types.h>

#include <errno.h>
#include <libgen.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

#include <librdkafka/rdkafka.h>

#include "ddtrace.h"

char *g_pname;
static int run = 1;

static void	usage(FILE *);
static void 	stop(int);

/*ARGSUSED*/
static void
stop(int sig)
{

	run = 0;
}

static void 
usage(FILE * fp)
{

	(void) fprintf(fp, "Usage: %s -b brokers -i input_topic "
	    "-o output_topic\n", g_pname);
}

/*
 * Prototype distributed dtrace agent.
 * The agent reveices D-Language scripts from a Apache Kafka topic. For each
 * script a new processes is forked. The process compiles the D-Language script
 * and sends the resulting DOF file to the kernel for execution. Results are
 * returned through another Apache Kafka topic.
 */
int
main(int argc, char *argv[])
{
	rd_kafka_t * rk;
	rd_kafka_topic_t * rkt;
	rd_kafka_conf_t * conf;
	rd_kafka_topic_conf_t * topic_conf;
	rd_kafka_message_t ** rkmessages = NULL;
	int64_t start_offset = RD_KAFKA_OFFSET_END;
	int c;
	int partition = 0;
	int pid;
	char errstr[512];
	char * brokers;
	char * topic;
	char * output_topic;

	g_pname = basename(argv[0]); 	

	if (argc == 1) {
		usage(stderr);
		exit(EXIT_FAILURE);
	}

	opterr = 0;
	while ((c = getopt(argc, argv, "b:i:o:")) != -1) {
		switch(c) {
		case 'b':
			brokers = optarg;
			break;
		case 'i':
			topic = optarg;
			break;
		case 'o':
			output_topic = optarg;
			break;
		case '?':
		default:
			usage(stderr);
			exit(EXIT_FAILURE);
		}
	}

	if (brokers == NULL || topic == NULL ) {
		usage(stderr);
		exit(EXIT_FAILURE);
	}

	signal(SIGINT, stop);

	/*
	 * Setup the Kafka topic used for receiving receiving D-Langauge
	 * instrumentation scripts
	 */
	conf = rd_kafka_conf_new();

	topic_conf = rd_kafka_topic_conf_new();

	if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr,
	    sizeof(errstr)))) {
		fprintf(stderr, "%s: failed to create Kafka consumer: %s\n",
		    g_pname, errstr);
		exit(EXIT_FAILURE);
	}

	if (rd_kafka_brokers_add(rk, brokers) < 1) {
		fprintf(stderr, "%s: no valid brokers specified\n", g_pname);
		exit(EXIT_FAILURE);
	}

	if (!(rkt = rd_kafka_topic_new(rk, topic, topic_conf))) {
		fprintf(stderr, "%s: failed to create Kafka topic %s\n",
		    g_pname, topic);
		exit(EXIT_FAILURE);
	}

	if (rd_kafka_consume_start(rkt, partition, start_offset) == -1) {
		fprintf(stderr, "%s: failed to start consuming: %s\n",
		    g_pname, rd_kafka_err2str(rd_kafka_errno2err(errno)));
		if (errno == EINVAL) {
	        	fprintf(stderr, "%s: broker based offset storage "
			    "requires a group.id, "
			    "add: -X group.id=yourGroup\n", g_pname);
		}
		exit(EXIT_FAILURE);
	}

	/**
	 * Consume D-Language scripts from the Kafka topic, fork a seperate
	 * process to compile and send to the kernel for execution.
	 */
	while (run) {
		rd_kafka_message_t * rkmessage;
		rd_kafka_resp_err_t err;

		rd_kafka_poll(rk ,0);

		rkmessage = rd_kafka_consume(rkt, partition, 1000);
		if (!rkmessage) {
			continue;
		}

		if (!rkmessage->err && rkmessage->len > 0) {
			char * buffer = alloca((int) rkmessage->len) + 1;
			memcpy(buffer, (char *) rkmessage->payload,
			    rkmessage->len);
			buffer[(int) rkmessage->len] = '\0';

			pid = fork();
			if (pid < 0) {
				perror("ERROR on fork");
				exit(EXIT_FAILURE);
			} else if (pid == 0) {
				ddtrace(buffer, brokers, output_topic);
				fprintf(stdout,
				    "%s: dtrace child process closed\n",
				    g_pname);
				exit(EXIT_SUCCESS);
			} else {
				rd_kafka_message_destroy(rkmessage);
			}
		}
	}

	/* Stop consuming from the Kafka topic*/
	fprintf(stdout, "%s: stop Kafka consumming from topic\n", g_pname);
	if (rd_kafka_consume_stop(rkt, partition) == -1) {
		fprintf(stderr, "%s: failed stopping Kafka consumer\n",
		    g_pname);
	}

	while (rd_kafka_outq_len(rk) > 0) {
		rd_kafka_poll(rk, 10);
	}

	/* Destroy the Kafka topic */
	fprintf(stdout, "%s: destroying Kafka topic\n", g_pname);
	rd_kafka_topic_destroy(rkt);

	/* Destroy the Kafka handle */
	fprintf(stdout, "%s: destroying Kafka handle\n", g_pname);
	rd_kafka_destroy(rk);

	return EXIT_SUCCESS;
}

