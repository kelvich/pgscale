#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "pgstat.h"

#include <arpa/inet.h>
#include <unistd.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define REQEST_MAX_SIZE 1024

PG_FUNCTION_INFO_V1(pgscale_start);
PG_FUNCTION_INFO_V1(pgscale_stop);

void _PG_init(void);
void pgscale_http_main(Datum arg) pg_attribute_noreturn();

static BackgroundWorker pgscale_http_worker = {
	"pgscale_http_worker",
	BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
	BgWorkerStart_RecoveryFinished,
	BGW_NEVER_RESTART,
	pgscale_http_main
};

void
_PG_init(void)
{
	// XXX: add some connection info to GUC variables
	RegisterBackgroundWorker(&pgscale_http_worker);
}

static void
pgscale_sigterm(SIGNAL_ARGS)
{
	// XXX: sometimes I can't bind to socket some time after restart
	// probably data on socket (if some) should be read before exit.
	proc_exit(0);
}

static int
pgscale_run_sql(char *pg_view_name, StringInfo response) {
	char select_sql[60] = "select * from ";
	int ret;

	if (strlen(pg_view_name) > 40)
		return -1;

	(void) strncat(select_sql, pg_view_name, strlen(pg_view_name));

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(select_sql, true, 0);

	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_tuptable != NULL)
	{
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;
		int proc = SPI_processed;
		int i, j;

		appendStringInfo(response, "[");
		for (j = 0; j < proc; j++)
		{
			HeapTuple tuple = tuptable->vals[j];

			appendStringInfo(response, "{");
			for (i = 1; i <= tupdesc->natts; i++)
				appendStringInfo(response,
					"%s : %s, ", SPI_fname(tupdesc, i), SPI_getvalue(tuple, tupdesc, i));
			appendStringInfo(response, "}, ");
		}
		appendStringInfo(response, "]");
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	return 0;
}

static StringInfo
pgscale_http_handle_request(char *buf, int len) {
	StringInfo response = makeStringInfo();

	/* rfc2616 grammar tokens */
	char *request_line, *method, *request_uri; 

	/*
	 * We are interested only in Request-Line out of all Request, so skip
	 * following headers and possible message-body.
	 */
	request_line = strsep(&buf, "\r\n");
	// XXX: add some checks for malformed request
	fprintf(stderr, "request_line: '%s'\n", request_line);

	method = strsep(&request_line, " ");
	request_uri = strsep(&request_line, " ");

	fprintf(stderr, "method: '%s'\n", method);
	fprintf(stderr, "request_uri: '%s'\n", request_uri);

	/* Reply with 'Not Implemented' for any other method except GET */
	if (strcmp(method, "GET"))
	{
		appendStringInfo(response, "HTTP/1.1 501 Not Implemented\r\n\r\n");
		return response;
	}

	/*
	 * Catch request to specified scope and return corresponding view output
	 * as json.
	 */
	if (!strncmp(request_uri, "/pgstat/", 8))
	{
		char *pg_view_name = request_uri + 8;
		// int ret;

		// fnmatch here

		// XXX: add here whitelist of sql views
		// XXX: add error handling in pgscale_run_sql() (?)
		appendStringInfo(response, "HTTP/1.1 200 OK\r\n\r\n");
		pgscale_run_sql(pg_view_name, response);
		appendStringInfo(response, "\r\n\r\n");
		return response;
	}

	/*
	 * Here we assume that request_uri is path to file.
	 */
	appendStringInfo(response, "HTTP/1.1 404 Not found\r\n\r\n");
	return response;
}

void
pgscale_http_main(Datum arg)
{
	int s, cli_fd;
	int err;
	socklen_t addr_size;
	struct sockaddr_in sa;
	struct sockaddr_storage their_addr;

	fprintf(stderr, "%s\n", "pgscale_http_main called");

	pqsignal(SIGTERM, pgscale_sigterm);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection("postgres", NULL);

	s = socket(PF_INET, SOCK_STREAM, 0);
	if (s < 0) {
		printf("Socket error.\n");
		proc_exit(1);
	}

	sa.sin_family = AF_INET;
	sa.sin_port = htons(1137);
	sa.sin_addr.s_addr = inet_addr("127.0.0.1");
	err = bind(s, (struct sockaddr *)&sa, sizeof sa);
	if (err != 0) {
		printf("Can't bind to socket.\n");
		proc_exit(1);
	}

	err = listen(s, 10);
	if (err < 0) {
		printf("Can't listen socket.\n");
		proc_exit(1);
	}

	while (1) {
		StringInfo msg = makeStringInfo();
		char buf[REQEST_MAX_SIZE];
		int bytes_sent, bytes_recv;

		addr_size = sizeof their_addr;
		cli_fd = accept(s, (struct sockaddr *) &their_addr, &addr_size);
		printf("Somebody connected\n");

		/*
		 * Here I assume that request fit REQEST_MAX_SIZE.
		 * Probably socket should be read until '\r\n\r\n' sequense is found.
		 */
		bytes_recv = recv(cli_fd, buf, REQEST_MAX_SIZE, 0);
		if (bytes_recv < 0) {
			printf("Can't receive. %s\n", strerror(errno));
		}

		fprintf(stderr, ">>>>>>>>>\n%.*s>>>>>>>>>\n", bytes_recv, buf);
		msg = pgscale_http_handle_request(buf, bytes_recv);
		fprintf(stderr, "<<<<<<<<<\n%s<<<<<<<<<\n\n", msg->data);

		bytes_sent = send(cli_fd, msg->data, msg->len, 0);
		pfree(msg->data);
		close(cli_fd);
	}

	shutdown(s, SHUT_RDWR);
	close(s);

	proc_exit(1);
}
