/*-
 * Copyright (c) 2018 (Graeme Jenkinson)
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

#include <dt_impl.h>
#include <errno.h>
#include <libgen.h>
#include <librdkafka/rdkafka.h>
#include <signal.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dl_assert.h"

static int dtc_get_buf(dtrace_hdl_t *, int, dtrace_bufdesc_t **);
static void dtc_put_buf(dtrace_hdl_t *, dtrace_bufdesc_t *b);

static char *g_pname;
static int g_status = 0;
static int g_intr;
static rd_kafka_t *rk;
static rd_kafka_topic_t *rkt;

static inline void 
dtc_usage(FILE * fp)
{

	(void) fprintf(fp,
	    "Usage: %s -b brokers -t input_topic -s script\n", g_pname);
}

/*ARGSUSED*/
static inline void
dtc_intr(int signo)
{

	g_intr = 1;
}
	
/*ARGSUSED*/
static int
chew(const dtrace_probedata_t *data, void *arg)
{
#ifndef NDEBUG
	dtrace_probedesc_t *pd = data->dtpda_pdesc;
	dtrace_eprobedesc_t *ed = data->dtpda_edesc;
	processorid_t cpu = data->dtpda_cpu;

	fprintf(stdout, "dtpd->id = %u\n", pd->dtpd_id);
	fprintf(stdout, "dtepd->id = %u\n", ed->dtepd_epid);
	fprintf(stdout, "dtpd->func = %s\n", pd->dtpd_func);
	fprintf(stdout, "dtpd->name = %s\n", pd->dtpd_name);
#endif

	return (DTRACE_CONSUME_THIS);
}
	
/*ARGSUSED*/
static int
chewrec(const dtrace_probedata_t * data, const dtrace_recdesc_t * rec,
    void * arg)
{
	dtrace_actkind_t act;
	uintptr_t addr;

	/* Check if the final record has been processed. */
	if (rec == NULL) {

		return (DTRACE_CONSUME_NEXT); 
	}

#ifndef NDEBUG
	fprintf(stdout, "chewrec %p\n", rec);
	fprintf(stdout, "dtrd_action %u\n", rec->dtrd_action);
	fprintf(stdout, "dtrd_size %u\n", rec->dtrd_size);
	fprintf(stdout, "dtrd_alignment %u\n", rec->dtrd_alignment);
	fprintf(stdout, "dtrd_format %u\n", rec->dtrd_format);
	fprintf(stdout, "dtrd_arg  %lu\n", rec->dtrd_arg);
	fprintf(stdout, "dtrd_uarg  %lu\n", rec->dtrd_uarg);
#endif

	act = rec->dtrd_action;
	addr = (uintptr_t)data->dtpda_data;

	if (act == DTRACEACT_EXIT) {
		g_status = *((uint32_t *) addr);
		return (DTRACE_CONSUME_NEXT);
	}

	return (DTRACE_CONSUME_THIS); 
}

/*
 * Prototype distributed dtrace agent.
 * The agent recieves DTrace records from an Apache Kafka topic and prints
 * them using libdtrace.
 */
int
main(int argc, char *argv[])
{
	dtrace_consumer_t con;
	dtrace_prog_t * prog;
	dtrace_proginfo_t info;
	dtrace_hdl_t *dtp;
	FILE *fp;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	int64_t start_offset = RD_KAFKA_OFFSET_BEGINNING;
	int c, err, partition = 0, ret = 0, script_argc = 0;
	char *args, *brokers, *topic_name;
	char **script_argv;
	char errstr[512];

	g_pname = basename(argv[0]); 	

	/** Allocate space required for any arguments being passed to the
	 *  D-language script.
	 */
	script_argv = (char **) malloc(sizeof(char *) * argc);
	if (script_argv == NULL) {

		fprintf(stderr, "%s: failed to allocate script arguments\n",
		    g_pname);
		exit(EXIT_FAILURE);
	}

	opterr = 0;
	for (optind = 0; optind < argc; optind++) {
		while ((c = getopt(argc, argv, "b:t:s:")) != -1) {
			switch(c) {
			case 'b':
				brokers = optarg;
				break;
			case 't':
				topic_name = optarg;
				break;
			case 's':
				if ((fp = fopen(optarg, "r")) == NULL) {

					fprintf(stderr,
					    "%s: failed to open script file "
					    "%s\n", optarg, g_pname);
					ret = -1;
					goto free_script_args;
				}
				break;
			case '?':
			default:
				dtc_usage(stderr);
				ret = -1;
				goto free_script_args;
			}
		}

		if (optind < argc)
			script_argv[script_argc++] = argv[optind];
	}

	if (brokers == NULL || topic_name == NULL || fp == NULL) {

		dtc_usage(stderr);
		ret = -1;
		goto free_script_args;
	}

	/* Setup the Kafka topic used for receiving DTrace records. */
	conf = rd_kafka_conf_new();
	if (conf == NULL) {

		fprintf(stderr, "%s: failed to create Kafka conf: %s\n",
		    g_pname, rd_kafka_err2str(rd_kafka_last_error()));
		ret = -1;
		goto free_script_args;
	}

	if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr,
	    sizeof(errstr)))) {

		fprintf(stderr, "%s: failed to create Kafka consumer: %s\n",
		    g_pname, errstr);
		goto free_script_args;
	}

	if (rd_kafka_brokers_add(rk, brokers) < 1) {

		fprintf(stderr, "%s: no valid brokers specified: %s\n",
		    g_pname, rd_kafka_err2str(rd_kafka_last_error()));
		ret = -1;
		goto destroy_kafka;
	}

	topic_conf = rd_kafka_topic_conf_new();
	if (topic_conf == NULL) {

		fprintf(stderr, "%s: failed to start consuming: %s\n",
		    g_pname, rd_kafka_err2str(rd_kafka_last_error()));
		ret = -1;
		goto destroy_kafka;
	}

	if (!(rkt = rd_kafka_topic_new(rk, topic_name, topic_conf))) {

		fprintf(stderr, "%s: failed to create Kafka topic %s: %s\n",
		    g_pname, topic_name,
		    rd_kafka_err2str(rd_kafka_last_error()));
		ret = -1;
		goto destroy_kafka;
	}

	if (rd_kafka_consume_start(rkt, partition, start_offset) == -1) {

		fprintf(stderr, "%s: failed to start consuming: %s\n",
		    g_pname, rd_kafka_err2str(rd_kafka_last_error()));
		if (errno == EINVAL) {
	        	fprintf(stderr, "%s: broker based offset storage "
			    "requires a group.id, "
			    "add: -X group.id=yourGroup\n", g_pname);
		}
		ret = -1;
		goto destroy_kafka;
	}
	
	con.dc_consume_probe = chew;
	con.dc_consume_rec = chewrec;
	con.dc_put_buf = dtc_put_buf;
	con.dc_get_buf = dtc_get_buf;

	if ((dtp = dtrace_open(DTRACE_VERSION, 0, &err)) == NULL) {

		fprintf(stderr, "%s: failed to initialize dtrace %s",
		    g_pname, dtrace_errmsg(dtp, dtrace_errno(dtp)));
		ret = -1;
		goto destroy_kafka;
	}
#ifndef NDEBUG
	fprintf(stdout, "%s: dtrace initialized\n", g_pname);
#endif

	(void) dtrace_setopt(dtp, "aggsize", "4m");
	(void) dtrace_setopt(dtp, "bufsize", "4k");
	(void) dtrace_setopt(dtp, "bufpolicy", "switch");
	(void) dtrace_setopt(dtp, "destructive", 0);
#ifndef NDEBUG
	printf("%s: dtrace options set\n", g_pname);
#endif

	if ((prog = dtrace_program_fcompile(dtp, fp,
	    DTRACE_C_PSPEC | DTRACE_C_CPP, script_argc, script_argv)) == NULL) {

		fprintf(stderr, "%s: failed to compile dtrace program %s",
		    g_pname, dtrace_errmsg(dtp, dtrace_errno(dtp)));
		ret = -1;
		goto destroy_dtrace;
	}
#ifndef NDEBUG
	fprintf(stdout, "%s: dtrace program compiled\n", g_pname);
#endif
	
	(void) fclose(fp);
	
	if (dtrace_program_exec(dtp, prog, &info) == -1) {

		fprintf(stderr, "%s: failed to enable dtrace probes %s",
		    g_pname, dtrace_errmsg(dtp, dtrace_errno(dtp)));
		ret = -1;
		goto destroy_dtrace;
	}
#ifndef NDEBUG
	fprintf(stdout, "%s: dtrace probes enabled\n", g_pname);
#endif

	struct sigaction act;
	(void) sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = dtc_intr;
	(void) sigaction(SIGINT, &act, NULL);
	(void) sigaction(SIGTERM, &act, NULL);

	int done = 0;
	do {
		if (!done || !g_intr)
			dtrace_sleep(dtp);	

		if (done || g_intr) {
			done = 1;
		}

		switch (dtrace_work_detached(dtp, stdout, &con, rkt)) {
		case DTRACE_WORKSTATUS_DONE:
			done = 1;
			break;
		case DTRACE_WORKSTATUS_OKAY:
			break;
		case DTRACE_WORKSTATUS_ERROR:
		default:
			if (dtrace_errno(dtp) != EINTR) 
				fprintf(stderr, "%s : %s", g_pname,
				    dtrace_errmsg(dtp, dtrace_errno(dtp)));
				done = 1;
			break;
		}

	} while (!done);

destroy_dtrace:
	/* Destroy dtrace the handle. */
#ifndef NDEBUG
	fprintf(stdout, "%s: closing dtrace\n", g_pname);
#endif
	dtrace_close(dtp);

destroy_kafka:
	/* Destroy the Kafka topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy the Kafka handle. */
#ifndef NDEBUG
	fprintf(stdout, "%s: destroy kafka handle\n", g_pname);
#endif
	rd_kafka_destroy(rk);

free_script_args:	
	/* Free the memory used to hold the script arguments. */	
	free(script_argv);

	return ret;
}

static int
dtc_get_buf(dtrace_hdl_t *dtp, int cpu, dtrace_bufdesc_t **bufp)
{
	dtrace_optval_t size;
	dtrace_bufdesc_t *buf;
	rd_kafka_message_t *rkmessage;
	int partition = 0;
	
	DL_ASSERT(dtp != NULL, ("DTrace handle cannot be NULL"));
	DL_ASSERT(buf != NULL, ("Buffer instance to free cannot be NULL"));

	buf = dt_zalloc(dtp, sizeof(*buf));
	if (buf == NULL)
		return -1;

	/* Non-blocking poll of the log. */
	rd_kafka_poll(rk, 0);

	rkmessage = rd_kafka_consume(rkt, partition, 0);
	if (rkmessage != NULL) {

		if (!rkmessage->err && rkmessage->len > 0) {

#ifndef NDEBUG
			fprintf(stdout, "%s: message in log %zu\n",
			     g_pname, rkmessage->len);
#endif

			buf->dtbd_data = dt_zalloc(dtp, rkmessage->len);
			if (buf->dtbd_data == NULL) {

				dt_free(dtp, buf);
				return -1;
			}
			buf->dtbd_size = rkmessage->len;
			buf->dtbd_cpu = cpu;

			memcpy(buf->dtbd_data, rkmessage->payload,
			    rkmessage->len);
		} else {
#ifndef NDEBUG
			if (rkmessage->err ==
			    RD_KAFKA_RESP_ERR__PARTITION_EOF) {
				fprintf(stdout, "%s: no message in log\n",
				     g_pname);
			}
#endif
		}

		rd_kafka_message_destroy(rkmessage);
	}

	*bufp = buf;
	return 0;
}

static void
dtc_put_buf(dtrace_hdl_t *dtp, dtrace_bufdesc_t *buf)
{

	DL_ASSERT(dtp != NULL, ("DTrace handle cannot be NULL"));
	DL_ASSERT(buf != NULL, ("Buffer instance to free cannot be NULL"));
	DL_ASSERT(buf->dtbd_data != NULL,
	    ("Buffer data pointer cannot be NULL"));

	dt_free(dtp, buf->dtbd_data);
	dt_free(dtp, buf);
}
