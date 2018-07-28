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

#include <sys/types.h>
#include <sys/nv.h>

#include <dt_impl.h>
#include <errno.h>
#include <libgen.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

#include "dl_config.h"

static void dt_log_put_buf(dtrace_hdl_t *, dtrace_bufdesc_t *b);
static int dt_log_get_buf(dtrace_hdl_t *, int, dtrace_bufdesc_t **);
static void usage(FILE *);

static char *g_pname;
static int g_status = 0;
static int g_intr;
static dtrace_hdl_t * g_dtp;
static rd_kafka_t *rk;
static rd_kafka_topic_t *rkt;

static inline void 
usage(FILE * fp)
{

	(void) fprintf(fp,
	    "Usage: %s -b brokers -t input_topic -s script\n", g_pname);
}

/*ARGSUSED*/
static inline void
intr(int signo)
{

	g_intr = 1;
}
	
/*ARGSUSED*/
static int
chew(const dtrace_probedata_t *data, void *arg)
{
	dtrace_probedesc_t *pd = data->dtpda_pdesc;
	dtrace_eprobedesc_t *ed = data->dtpda_edesc;
	processorid_t cpu = data->dtpda_cpu;

	fprintf(stdout, "dtpd->id = %u\n", pd->dtpd_id);
	fprintf(stdout, "dtepd->id = %u\n", ed->dtepd_epid);
	fprintf(stdout, "dtpd->func = %s\n", pd->dtpd_func);
	fprintf(stdout, "dtpd->name = %s\n", pd->dtpd_name);

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

	fprintf(stdout, "chewrec %p\n", rec);
	fprintf(stdout, "dtrd_action %u\n", rec->dtrd_action);
	fprintf(stdout, "dtrd_size %u\n", rec->dtrd_size);
	fprintf(stdout, "dtrd_alignment %u\n", rec->dtrd_alignment);
	fprintf(stdout, "dtrd_format %u\n", rec->dtrd_format);
	fprintf(stdout, "dtrd_arg  %lu\n", rec->dtrd_arg);
	fprintf(stdout, "dtrd_uarg  %lu\n", rec->dtrd_uarg);

	act = rec->dtrd_action;
	addr = (uintptr_t)data->dtpda_data;

	if (act == DTRACEACT_EXIT) {
		g_status = *((uint32_t *) addr);
		return (DTRACE_CONSUME_NEXT);
	}

	return (DTRACE_CONSUME_THIS); 
}

/*PRINTFLIKE1*/
static void
dfatal(const char *fmt, ...)
{
#if !defined(illumos) && defined(NEED_ERRLOC)
        char *p_errfile = NULL;
        int errline = 0;
#endif
        va_list ap;

        va_start(ap, fmt);

        (void) fprintf(stderr, "%s: ", g_pname);
        if (fmt != NULL)
                (void) vfprintf(stderr, fmt, ap);

        va_end(ap);

        if (fmt != NULL && fmt[strlen(fmt) - 1] != '\n') {
                (void) fprintf(stderr, ": %s\n",
                    dtrace_errmsg(g_dtp, dtrace_errno(g_dtp)));
        } else if (fmt == NULL) {
                (void) fprintf(stderr, "%s\n",
                    dtrace_errmsg(g_dtp, dtrace_errno(g_dtp)));
        }
#if !defined(illumos) && defined(NEED_ERRLOC)
        dt_get_errloc(g_dtp, &p_errfile, &errline);
        if (p_errfile != NULL)
                printf("File '%s', line %d\n", p_errfile, errline);
#endif

        /*
         * Close the DTrace handle to ensure that any controlled processes are
         * correctly restored and continued.
         */
        dtrace_close(g_dtp);

        exit(EXIT_FAILURE);
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
	dtrace_consumer_t con;
	FILE *fp;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	int64_t start_offset = RD_KAFKA_OFFSET_BEGINNING;
	char *brokers, *topic_name;
	char *script = "tick-2sec { trace(1); } tick-1sec { printf(\"hello\"); }";
	int c, err, partition = 0;
	char errstr[512];

	g_pname = basename(argv[0]); 	

	if (argc == 1) {
		usage(stderr);
		exit(EXIT_FAILURE);
	}

	opterr = 0;
	while ((c = getopt(argc, argv, "b:t:s:")) != -1) {
		switch(c) {
		case 'b':
			brokers = optarg;
			break;
		case 't':
			topic_name = optarg;
			break;
		case 's':
			if ((fp = fopen(optarg, "r")) == NULL)
			    exit(EXIT_FAILURE);
			break;
		case '?':
		default:
			usage(stderr);
			exit(EXIT_FAILURE);
		}
	}

	if (brokers == NULL || topic_name == NULL || fp == NULL) {
		usage(stderr);
		exit(EXIT_FAILURE);
	}

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

	if (!(rkt = rd_kafka_topic_new(rk, topic_name, topic_conf))) {
		fprintf(stderr, "%s: failed to create Kafka topic %s\n",
		    g_pname, topic_name);
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
	
	con.dc_consume_probe = chew;
	con.dc_consume_rec = chewrec;
	con.dc_put_buf = dt_log_put_buf;
	con.dc_get_buf = dt_log_get_buf;

	if ((g_dtp = dtrace_open(DTRACE_VERSION, 0, &err)) == NULL) {
		fprintf(stderr, "%s failed to initialize dtrace: %s\n",
		    g_pname, dtrace_errmsg(g_dtp, err));
		exit(EXIT_FAILURE);
	}
	fprintf(stdout, "%s: dtrace initialized\n", g_pname);

	(void) dtrace_setopt(g_dtp, "aggsize", "4m");
	(void) dtrace_setopt(g_dtp, "bufsize", "4k");
	(void) dtrace_setopt(g_dtp, "bufpolicy", "switch");
	(void) dtrace_setopt(g_dtp, "destructive", 0);
	printf("%s: dtrace options set\n", g_pname);

	dtrace_prog_t * prog;
	char *newargs[] = {"g", "\"host\""};
	if ((prog = dtrace_program_fcompile(g_dtp, fp,
		DTRACE_C_PSPEC | DTRACE_C_CPP, 2, newargs)) == NULL) {
				fprintf(stderr, "%s",
				    dtrace_errmsg(g_dtp, dtrace_errno(g_dtp)));
		dfatal("failed to compile dtrace program: %s\n", script);
	}
	fprintf(stdout, "%s: dtrace program compiled\n", g_pname);
/*
	dtrace_prog_t * prog;
	if ((prog = dtrace_program_strcompile(g_dtp, script,
		DTRACE_PROBESPEC_NAME, DTRACE_C_PSPEC, 0, NULL)) == NULL) {
		dfatal("failed to compile dtrace program: %s\n", script);
	}
	fprintf(stdout, "%s: dtrace program compiled\n", g_pname);
*/
	dtrace_proginfo_t info;
	if (dtrace_program_exec(g_dtp, prog, &info) == -1) {
		dfatal("failed to enable dtrace probes\n");
	}
	fprintf(stdout, "%s: dtrace probes enabled\n", g_pname);

	struct sigaction act;
	(void) sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = intr;
	(void) sigaction(SIGINT, &act, NULL);
	(void) sigaction(SIGTERM, &act, NULL);

	int done = 0;
	do {
		if (done || g_intr) {
			done = 1;
		}
		
		switch (dtrace_work_detached(g_dtp, stdout, &con, rkt)) {
		case DTRACE_WORKSTATUS_DONE:
			done = 1;
			break;
		case DTRACE_WORKSTATUS_OKAY:
			break;
		case DTRACE_WORKSTATUS_ERROR:
		default:
			if (dtrace_errno(g_dtp) != EINTR) 
				fprintf(stderr, "%s",
				    dtrace_errmsg(g_dtp, dtrace_errno(g_dtp)));
				dfatal("processing aborted\n");
			break;
		}

	} while (!done);

destroy_kafka:
	/* Poll to handle delivery reports. */
	rd_kafka_poll(rk, 0);

	/* Wait for messages to be delivered. */
	while (rd_kafka_outq_len(rk) > 0) {
		rd_kafka_poll(rk, 100);
	}

	/* Destroy the Kafka topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy the Kafka handle. */
	fprintf(stdout, "%s: destroy kafka handle\n", g_pname);
	rd_kafka_destroy(rk);

destroy_dtrace:
	/* Destroy dtrace the handle. */
	fprintf(stdout, "%s: closing dtrace\n", g_pname);
	dtrace_close(g_dtp);

	return (0);
}

static void
dt_log_put_buf(dtrace_hdl_t *dtp, dtrace_bufdesc_t *buf)
{

	dt_free(dtp, buf->dtbd_data);
	dt_free(dtp, buf);
}

static int
dt_log_get_buf(dtrace_hdl_t *dtp, int cpu, dtrace_bufdesc_t **bufp)
{
	dtrace_optval_t size;
	dtrace_bufdesc_t *buf;
	rd_kafka_message_t *rkmessage;
	int partition = 0;

	buf = dt_zalloc(dtp, sizeof(*buf));
	if (buf == NULL)
		return -1;

	(void) dtrace_getopt(dtp, "bufsize", &size);
	buf->dtbd_data = dt_zalloc(dtp, size);
	if (buf->dtbd_data == NULL) {
		dt_free(dtp, buf);
		return -1;
	}
	buf->dtbd_size = size;
	buf->dtbd_cpu = cpu;

	/* Non-blocking poll of the log. */
	rd_kafka_poll(rk, 1000);

	rkmessage = rd_kafka_consume(rkt, partition, 0);
	if (rkmessage != NULL) {

		if (!rkmessage->err && rkmessage->len > 0) {

			fprintf(stdout, "%s: message in log %zu\n",
			     g_pname, rkmessage->len);

			/*
			buf->dtbd_data = dt_zalloc(dtp, rkmessage->len);
			if (buf->dtbd_data == NULL) {
				dt_free(dtp, buf);
				return -1;
			}
			buf->dtbd_size = size;
			buf->dtbd_cpu = cpu;

			memcpy(buf->dtbd_data, rkmessage->payload,
			    rkmessage->len);

			*bufp = buf;
			return 0;
			*/

			// TODO: ensure that the message fits within the
			// buffer
			if (rkmessage->len <= buf->dtbd_size)
				memcpy(buf->dtbd_data,
				    rkmessage->payload, rkmessage->len);
		} else {
			if (rkmessage->err ==
			    RD_KAFKA_RESP_ERR__PARTITION_EOF) {
				fprintf(stdout, "%s: no message in log\n",
				     g_pname);
			}
		}

		rd_kafka_message_destroy(rkmessage);
	}

	*bufp = buf;
	return 0;
}
