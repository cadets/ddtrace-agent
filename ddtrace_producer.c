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
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

#include "dlog.h"
#include "dl_config.h"

static char *g_pname;
static int g_status = 0;
static int g_intr;
static dtrace_hdl_t *g_dtp;

static inline void 
usage(FILE * fp)
{

	(void) fprintf(fp, "Usage: %s -t topic -s script\n", g_pname);
}

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

/*
 * Prototype distributed dtrace agent.
 */
int
main(int argc, char *argv[])
{
	struct dl_client_config_desc *client_conf;
	dtrace_consumer_t con;
	FILE *fp;
	nvlist_t *props;
	char *topic_name;
	size_t packed_len;
	int c, dlog, done = 0, err, ret = 0, rc, script_argc = 0;
	char konarg[13];
	char **script_argv;

	g_pname = basename(argv[0]); 	

	script_argv = malloc(sizeof(char *) * argc);
	if (script_argv == NULL) {

		fprintf(stderr, "%s: failed to allocate script arguments\n",
		    g_pname);
		exit(EXIT_FAILURE);
	}

	opterr = 0;
	for (optind = 0; optind < argc; optind++) { 
		while ((c = getopt(argc, argv, "t:s:")) != -1) {
			switch(c) {
			case 't':
				topic_name = optarg;
				break;
			case 's':
				fp = fopen(optarg, "r");
				if (fp == NULL) {

					fprintf(stderr,
					    "%s: failed to open script file "
					    "%s\n", optarg, g_pname);
					ret = -1;
					goto free_script_args;
				}
				break;
			case '?':
			default:
				usage(stderr);
				ret = -1;
				goto free_script_args;
			}
		}

		if (optind < argc)
			script_argv[script_argc++] = argv[optind]; 
	}

	if (topic_name == NULL || fp == NULL) {

		usage(stderr);
		ret = -1;
		goto free_script_args;
	}

	dlog = open("/dev/dlog", O_RDWR);
	if (dlog == -1) {

		fprintf(stderr, "%s failed to open dev dlog: %d\n",
		    g_pname, errno);
		ret = -1;
		(void) fclose(fp);
		goto free_script_args;
	}

	props = nvlist_create(0);
	if (props == NULL) {

		fprintf(stderr, "%s failed to create nvlist : %d\n",
		    g_pname, errno);
		ret = -1;
		goto close_dlog;
	}

	nvlist_add_string(props, DL_CONF_TOPIC, topic_name);

	client_conf = (struct dl_client_config_desc *) malloc(
	    sizeof(struct dl_client_config_desc));
	if (client_conf == NULL) {

		fprintf(stderr, "%s failed to allocate client config: %d\n",
		    g_pname, errno);
		ret = -1;
		goto destroy_nvlist;
	}

	client_conf->dlcc_packed_nvlist = nvlist_pack(props, &packed_len); 
	client_conf->dlcc_packed_nvlist_len = packed_len;

	rc = ioctl(dlog, DLOGIOC_PRODUCER, &client_conf);	
	if (rc != 0) {

		fprintf(stderr, "%s failed to create DLog producer: %d\n",
		    g_pname, errno);
		ret = -1;
		goto destroy_nvlist;
	}

	con.dc_consume_probe = NULL; // ?chew;
	con.dc_consume_rec = NULL; //chewrec;
	con.dc_put_buf = NULL; 
	con.dc_get_buf = NULL;

	if ((g_dtp = dtrace_open(DTRACE_VERSION, 0, &err)) == NULL) {

		fprintf(stderr, "%s failed to initialize dtrace: %s\n",
		    g_pname, dtrace_errmsg(g_dtp, err));
		ret = -1;
		goto destroy_dtrace;
	}
	fprintf(stdout, "%s: dtrace initialized\n", g_pname);

	(void) dtrace_setopt(g_dtp, "aggsize", "4m");
	(void) dtrace_setopt(g_dtp, "bufsize", "4k");
	(void) dtrace_setopt(g_dtp, "bufpolicy", "switch");
	sprintf(konarg, "%d", dlog);
	(void) dtrace_setopt(g_dtp, "konarg", konarg);
	printf("%s: dtrace options set\n", g_pname);

	dtrace_prog_t * prog;
	dtrace_proginfo_t info;
	if ((prog = dtrace_program_fcompile(g_dtp, fp,
	    DTRACE_C_PSPEC | DTRACE_C_CPP, script_argc, script_argv)) == NULL) {

		fprintf(stderr, "%s: failed to compile dtrace program %s",
		    g_pname, dtrace_errmsg(g_dtp, dtrace_errno(g_dtp)));
		ret = -1;
		goto destroy_dtrace;
		
	}
	fprintf(stdout, "%s: dtrace program compiled\n", g_pname);

	if (dtrace_program_exec(g_dtp, prog, &info) == -1) {

		fprintf(stderr, "failed to enable dtrace probes\n");
		ret = -1;
		goto destroy_dtrace;
	}
	fprintf(stdout, "%s: dtrace probes enabled\n", g_pname);

	struct sigaction act;
	(void) sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = intr;
	(void) sigaction(SIGINT, &act, NULL);
	(void) sigaction(SIGTERM, &act, NULL);

	if (dtrace_go(g_dtp) != 0) {
		
		fprintf(stderr, "could not start dtrace instrumentation\n");
		ret = -1;
		goto destroy_dtrace;
	}
	fprintf(stdout, "%s: dtrace instrumentation started...\n", g_pname);
 
	do {
		if (!g_intr && !done) {
			dtrace_sleep(g_dtp);
		}

		if (done || g_intr) {
			done = 1;
			if (dtrace_stop(g_dtp) == -1) {

				fprintf(stderr, "could not stop tracing\n");
				ret = -1;
			}
		}
	} while (!done);

destroy_dtrace:
	/* Destroy dtrace the handle. */
	fprintf(stdout, "%s: closing dtrace\n", g_pname);
	dtrace_close(g_dtp);

destroy_nvlist:
	/* Destory the nvlist used to store the Dlog producer arguments. */
	nvlist_destroy(props);

close_dlog:
	/* Destroy dlog the handle. */
	fprintf(stdout, "%s: closing dlog\n", g_pname);
	close(dlog);

	/* Close the DTrace script file handle. */	
	(void) fclose(fp);

free_script_args:	
	/* Free the memory used to hold the script arguments. */	
	free(script_argv);

	return ret;
}

