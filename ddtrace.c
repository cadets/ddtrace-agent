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

#include <sys/dtrace.h>

#include <errno.h>
#include <dtrace.h>
#include <dt_impl.h>
#include <dt_printf.h>
#include <signal.h>
#include <stdio.h>
#include <syslog.h>
#include <unistd.h>

#include <libxo/xo.h>
#include <librdkafka/rdkafka.h>

#include "ddtrace.h"

/* FreeBSD does not provide the #define HOST_NAME_MAX (and is, in this regard,
 * not POSIX compliant). The maximum hostname length can be read as
 * sysconf(_SC_HOST_NAME_MAX).
 */
#define HOST_NAME_MAX	MAXHOSTNAMELEN

#define	BUFDUMPHDR(hdr) \
	(void) printf("%s: %s%s\n", g_pname, hdr, strlen(hdr) > 0 ? ":" : "");

#define	BUFDUMPSTR(ptr, field) \
	(void) printf("%s: %20s => ", g_pname, #field);	\
	if ((ptr)->field != NULL) {			\
		const char *c = (ptr)->field;		\
		(void) printf("\"");			\
		do {					\
			if (*c == '\n') {		\
				(void) printf("\\n");	\
				continue;		\
			}				\
							\
			(void) printf("%c", *c);	\
		} while (*c++ != '\0');			\
		(void) printf("\"\n");			\
	} else {					\
		(void) printf("<NULL>\n");		\
	}

#define	BUFDUMPASSTR(ptr, field, str) \
	(void) printf("%s: %20s => %s\n", g_pname, #field, str);

#define	BUFDUMP(ptr, field) \
	(void) printf("%s: %20s => %lld\n", g_pname, #field, \
	    (long long)(ptr)->field);

#define	BUFDUMPPTR(ptr, field) \
	(void) printf("%s: %20s => %s\n", g_pname, #field, \
	    (ptr)->field != NULL ? "<non-NULL>" : "<NULL>");

static const char * const NODE_JSON_NAME = "node";
static const char * const EVENT_JSON_NAME = "event";
static const char * const PROBE_JSON_NAME = "probe";
static int g_status = 0;
static int g_intr;
static dtrace_hdl_t * g_dtp;
static rd_kafka_t *rk;
extern char * g_pname;

static int	 bufhandler(const dtrace_bufdata_t *, void *);
static void 	 dfatal(const char *fmt, ...);
static void 	 msg_delivered(rd_kafka_t *, const rd_kafka_message_t *,
    void *);
static void 	 xo_ddt_close(void *);
static int 	 xo_ddt_flush(void *);
static int  	 xo_ddt_write(void *, const char *);

/*ARGSUSED*/
static void
intr(int signo)
{

	g_intr = 1;
}
	
/*ARGSUSED*/
static int
chew(const dtrace_probedata_t *data, void *arg)
{
	dtrace_probedesc_t *pd = data->dtpda_pdesc;
	processorid_t cpu = data->dtpda_cpu;
	static int heading;
	char hostname[HOST_NAME_MAX + 1];
	rd_kafka_topic_t *rkt = (rd_kafka_topic_t *) arg;

	/* Open new libxo handle */
	if (!(g_dtp->dt_xo_hdl = xo_create(XO_STYLE_JSON, XOF_WARN|XOF_DTRT))) {
		dfatal("failure creating libxo handle\n");
	}
	xo_set_writer(g_dtp->dt_xo_hdl, rkt, xo_ddt_write, xo_ddt_close,
		xo_ddt_flush);
	
        /* Serialise the probe description as JSON. */	
	gethostname(hostname, HOST_NAME_MAX);
	xo_open_container_h(g_dtp->dt_xo_hdl, NODE_JSON_NAME);
	xo_emit_h(g_dtp->dt_xo_hdl, "{:hostname/%s}", hostname);
	xo_close_container_hd(g_dtp->dt_xo_hdl);
	xo_open_container_h(g_dtp->dt_xo_hdl, PROBE_JSON_NAME);
	xo_emit_h(g_dtp->dt_xo_hdl,
	    "{:timestamp/%U} {:cpu/%d} {:id/%d} {:func/%s} {:name/%s}",
	    data->dtpda_timestamp, cpu, pd->dtpd_id, pd->dtpd_func,
	    pd->dtpd_name);
	xo_close_container_hd(g_dtp->dt_xo_hdl);

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
		xo_finish_h(g_dtp->dt_xo_hdl);
		xo_destroy(g_dtp->dt_xo_hdl);
		g_dtp->dt_xo_hdl = NULL;

		return (DTRACE_CONSUME_NEXT); 
	}

	act = rec->dtrd_action;
	addr = (uintptr_t)data->dtpda_data;

	if (act == DTRACEACT_EXIT) {
		g_status = *((uint32_t *) addr);
		return (DTRACE_CONSUME_NEXT);
	}

	return (DTRACE_CONSUME_THIS); 
}

/*ARGSUSED*/
static int
bufhandler(const dtrace_bufdata_t *bufdata, void *arg)
{
	const dtrace_aggdata_t *agg = bufdata->dtbda_aggdata;
	const dtrace_recdesc_t *rec = bufdata->dtbda_recdesc;
	const dtrace_probedesc_t *pd;
	uint32_t flags = bufdata->dtbda_flags;
	char buf[512], *c = buf, *end = c + sizeof (buf);
	int i, printed;

	struct {
		const char *name;
		uint32_t value;
	} flagnames[] = {
	    { "AGGVAL",		DTRACE_BUFDATA_AGGVAL },
	    { "AGGKEY",		DTRACE_BUFDATA_AGGKEY },
	    { "AGGFORMAT",	DTRACE_BUFDATA_AGGFORMAT },
	    { "AGGLAST",	DTRACE_BUFDATA_AGGLAST },
	    { "???",		UINT32_MAX },
	    { NULL }
	};

	if (bufdata->dtbda_probe != NULL) {
		pd = bufdata->dtbda_probe->dtpda_pdesc;
	} else if (agg != NULL) {
		pd = agg->dtada_pdesc;
	} else {
		pd = NULL;
	}

	BUFDUMPHDR(">>> Called buffer handler");
	BUFDUMPHDR("");

	BUFDUMPHDR("  dtrace_bufdata");
	BUFDUMPSTR(bufdata, dtbda_buffered);
	BUFDUMPPTR(bufdata, dtbda_probe);
	BUFDUMPPTR(bufdata, dtbda_aggdata);
	BUFDUMPPTR(bufdata, dtbda_recdesc);

	(void) snprintf(c, end - c, "0x%x ", bufdata->dtbda_flags);
	c += strlen(c);

	for (i = 0, printed = 0; flagnames[i].name != NULL; i++) {
		if (!(flags & flagnames[i].value))
			continue;

		(void) snprintf(c, end - c,
		    "%s%s", printed++ ? " | " : "(", flagnames[i].name);
		c += strlen(c);
		flags &= ~flagnames[i].value;
	}

	if (printed)
		(void) snprintf(c, end - c, ")");

	BUFDUMPASSTR(bufdata, dtbda_flags, buf);
	BUFDUMPHDR("");

	if (pd != NULL) {
		BUFDUMPHDR("  dtrace_probedesc");
		BUFDUMPSTR(pd, dtpd_provider);
		BUFDUMPSTR(pd, dtpd_mod);
		BUFDUMPSTR(pd, dtpd_func);
		BUFDUMPSTR(pd, dtpd_name);
		BUFDUMPHDR("");
	}

	if (rec != NULL) {
		BUFDUMPHDR("  dtrace_recdesc");
		BUFDUMP(rec, dtrd_action);
		BUFDUMP(rec, dtrd_size);

		if (agg != NULL) {
			uint8_t *data;
			int lim = rec->dtrd_size;

			(void) sprintf(buf, "%d (data: ", rec->dtrd_offset);
			c = buf + strlen(buf);

			if (lim > sizeof (uint64_t))
				lim = sizeof (uint64_t);

			data = (uint8_t *)agg->dtada_data + rec->dtrd_offset;

			for (i = 0; i < lim; i++) {
				(void) snprintf(c, end - c, "%s%02x",
				    i == 0 ? "" : " ", *data++);
				c += strlen(c);
			}

			(void) snprintf(c, end - c,
			    "%s)", lim < rec->dtrd_size ? " ..." : "");
			BUFDUMPASSTR(rec, dtrd_offset, buf);
		} else {
			BUFDUMP(rec, dtrd_offset);
		}

		BUFDUMPHDR("");
	}

	if (agg != NULL) {
		dtrace_aggdesc_t *desc = agg->dtada_desc;

		BUFDUMPHDR("  dtrace_aggdesc");
		BUFDUMPSTR(desc, dtagd_name);
		BUFDUMP(desc, dtagd_varid);
		BUFDUMP(desc, dtagd_id);
		BUFDUMP(desc, dtagd_nrecs);
		BUFDUMPHDR("");
	}

	return (DTRACE_HANDLE_OK);
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

/*ARGSUSED*/
static void
msg_delivered(rd_kafka_t * rk, const rd_kafka_message_t * rkmessage,
    void * opaque)
{
	if (rkmessage->err) {
		fprintf(stderr, "%s: message delivery failed: %s\n",
		    g_pname, rd_kafka_message_errstr(rkmessage));
	} else {
		fprintf(stdout, "%s: message delivered (%zd bytes)\n",
		    g_pname, rkmessage->len);
	}
}

static void
xo_ddt_close(void * arg)
{

}

/*ARGSUSED*/
static int
xo_ddt_flush(void * arg)
{

	return (0);
}

static int
xo_ddt_write(void * arg, const char * buf)
{
	int partition = RD_KAFKA_PARTITION_UA;
	rd_kafka_topic_t * rkt = (rd_kafka_topic_t *) arg;

	if (rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
		(void *) buf, strlen(buf), NULL, 0, NULL) == -1) {
		fprintf(stderr, "%s: failed to produce to topic %s "
		    "partition %i: %s\n", g_pname, rd_kafka_topic_name(rkt),
		    partition, rd_kafka_err2str(rd_kafka_errno2err(errno)));

		rd_kafka_poll(rk, 0);
		return (-1);
	}

	fprintf(stdout, "%s: sent %zd bytes to topic %s partition %i\n",
	    g_pname, strlen(buf), rd_kafka_topic_name(rkt), partition);

	rd_kafka_poll(rk, 0);

	return (0);
}

/*
 * Compile and instrument using dtrace. 
 * Custome dtrace consumeer that serailises probe and records as JSON. The
 * JSON records are then written back to the specified Kafak topic.
 */
int
ddtrace(ddtrace_script_t script, ddtrace_broker_spec_t brokers,
    ddtrace_topic_t topic)
{
	rd_kafka_conf_t * conf;
	rd_kafka_topic_conf_t * topic_conf;
	rd_kafka_topic_t *rkt; /* Kafka topic that response is written to */
	int err;
	char errstr[512];

	if ((g_dtp = dtrace_open(DTRACE_VERSION, 0, &err)) == NULL) {
		fprintf(stderr, "%s failed to initialize dtrace: %s\n",
		    g_pname, dtrace_errmsg(g_dtp, err));
		exit(EXIT_FAILURE);
	}
	fprintf(stdout, "%s: dtrace initialized\n", g_pname);

	(void) dtrace_setopt(g_dtp, "oformat", "json");
	(void) dtrace_setopt(g_dtp, "bufsize", "4m");
	(void) dtrace_setopt(g_dtp, "aggsize", "4m");
	(void) dtrace_setopt(g_dtp, "temporal", "4m");
	printf("%s: dtrace options set\n", g_pname);

	dtrace_prog_t * prog;
	if ((prog = dtrace_program_strcompile(g_dtp, script,
		DTRACE_PROBESPEC_NAME, DTRACE_C_PSPEC, 0, NULL)) == NULL) {
		dfatal("failed to compile dtrace program: %s\n", script);
	}
	fprintf(stdout, "%s: dtrace program compiled\n", g_pname);

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

	if (dtrace_go(g_dtp) != 0) {
		dfatal("could not start dtrace instrumentation\n");
	}
	fprintf(stdout, "%s: dtrace instrumentation started...\n", g_pname);
 
	if (dtrace_handle_buffered(g_dtp, &bufhandler, NULL) == -1) {
		dfatal("failed to establish buffered handler\n");
	}

	/* Create a Kafka handle */
	if (!(conf = rd_kafka_conf_new())) {
		dfatal("failure creating kafka conf\n");
	}
	rd_kafka_conf_set_dr_msg_cb(conf, msg_delivered);

	if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr,
	    sizeof(errstr)))) {
		/* Destroy the Kafka topic */
		rd_kafka_destroy(rk);
		dfatal("failed to create new producer: %s\n", errstr);
	}
	rd_kafka_set_log_level(rk, LOG_DEBUG);

	if (rd_kafka_brokers_add(rk, brokers) == 0) {
		rd_kafka_destroy(rk);
		dfatal("no valid brokers specified\n");
	}
	topic_conf = rd_kafka_topic_conf_new();
	rkt = rd_kafka_topic_new(rk, topic, topic_conf);
		   
	int done = 0;
	do {
		if (!g_intr && !done) {
			dtrace_sleep(g_dtp);
		}

		if (done || g_intr) {
			done = 1;
			if (dtrace_stop(g_dtp) == -1) {
				dfatal("could not stop tracing\n");
			}
		}
		

		switch (dtrace_work(g_dtp, NULL, chew, chewrec, rkt)) {
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

destroy_libxo:
	/*
	 * Destroy the libxo handle (this will only happen if we are in the
	 * middle of processing a set of records).
	 */
	if (g_dtp->dt_xo_hdl != NULL) {
		xo_finish_h(g_dtp->dt_xo_hdl);
		xo_destroy(g_dtp->dt_xo_hdl);
	}

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

