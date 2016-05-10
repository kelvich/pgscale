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

void pgscale_http_main(Datum arg) pg_attribute_noreturn();

static BackgroundWorker pgscale_http_worker = {
	"pgscale_http_worker",
	BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
	BgWorkerStart_RecoveryFinished,
	BGW_NEVER_RESTART,
	NULL,
	"pgscale",
	"pgscale_http_main"
};

static void
pgscale_sigterm(SIGNAL_ARGS)
{
	proc_exit(0);
}

static void
pgscale_run_sql(char *sql, StringInfo response) {
	int ret;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql, true, 0);

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
}

static StringInfo
pgscale_http_handle_request(char *buf, int len) {
	char *sql = "select * from pg_stat_database";
	StringInfo response = makeStringInfo();

	appendStringInfo(response, "HTTP/1.1 200 OK\r\n\r\n");
	pgscale_run_sql(sql, response);
	appendStringInfo(response, "\r\n\r\n");

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
		// StringInfoData msg;
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

		msg = pgscale_http_handle_request(buf, bytes_recv);
		bytes_sent = send(cli_fd, msg->data, msg->len, 0);
		
		pfree(msg->data);

		close(cli_fd);
	}

	printf("Exiting\n");
	shutdown(s, SHUT_RDWR);
	close(s);

	proc_exit(1);
}

Datum
pgscale_start(PG_FUNCTION_ARGS)
{
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t pid;

	pgscale_http_worker.bgw_notify_pid = MyProcPid;
	if (!RegisterDynamicBackgroundWorker(&pgscale_http_worker, &handle))
		fprintf(stderr, "%s\n", "can't register http worker");

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	fprintf(stderr, "pgscale_start called: status=%d, pid=%d\n", status, pid);

	PG_RETURN_INT32(42);
}

