/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Event-driven Driver using libev, replacing the thread-per-customer model.
 * Based on the dbt-2 driver2 pattern.
 *
 * Instead of spawning one pthread per simulated customer, this implementation
 * forks a small number of worker processes (one per CPU core) and uses libev
 * ev_timer watchers to schedule transaction submission with pacing delays.
 *
 * DoTxn() remains synchronous (EGen's CCE library requires it).  The
 * multi-process model distributes this blocking across P processes so that
 * one customer's blocking send/recv does not stall all others.
 */

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sched.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>

#include "Driver2.h"
#include "DM.h"
#include "DBT5Consts.h"

using namespace std;

// Pacing timer callback -- fires once per pacing interval for each customer.
// Executes one transaction synchronously, then re-arms (or stops if time is
// up).
static void
pacing_cb(struct ev_loop *loop, ev_timer *w, int revents)
{
	struct customer_context *ctx = reinterpret_cast<struct customer_context *>(
			reinterpret_cast<char *>(w)
			- offsetof(struct customer_context, pacing));

	// Execute one transaction: picks the next type from the mix, sends
	// the request to BrokerageHouse, receives the response, logs timing.
	ctx->customer->DoTxn();

	// If the test duration has expired, stop this customer.
	if (time(NULL) >= ctx->stop_time) {
		ctx->customer->LogStopTime();
		ev_timer_stop(loop, w);
		return;
	}

	// When pacing delay is zero (repeat == 0.0), ev_timer is one-shot and
	// will not fire again automatically.  Explicitly re-arm so the customer
	// submits the next transaction immediately -- matching the threaded
	// driver's nanosleep(0) + loop behaviour.
	if (w->repeat == 0.) {
		ev_timer_set(w, 0., 0.);
		ev_timer_start(loop, w);
	}
	// Otherwise repeat > 0 and libev automatically re-invokes after the
	// pacing interval.
}

// Data Maintenance timer callback -- fires once per minute.
static void
dm_timer_cb(struct ev_loop *loop, ev_timer *w, int revents)
{
	struct dm_context *ctx = reinterpret_cast<struct dm_context *>(
			reinterpret_cast<char *>(w)
			- offsetof(struct dm_context, dm_timer));

	time_t start = time(NULL);
	ctx->dm->DoTxn();
	time_t elapsed = time(NULL) - start;

	if (time(NULL) >= ctx->stop_time) {
		ctx->dm_sut->logStopTime();
		ev_timer_stop(loop, w);
		cout << "Data-Maintenance stopped." << endl;
		return;
	}

	// Schedule the next DM transaction so that the interval from start to
	// start is 60 seconds.  If the transaction itself took longer than 60s,
	// fire immediately.
	double remaining = (elapsed < 60) ? (60.0 - (double) elapsed) : 0.0;
	ev_timer_set(w, remaining, 0.0);
	ev_timer_start(loop, w);
}

// Return the number of online CPUs.  Falls back to 1 if unavailable.
static int
get_num_cpus()
{
	long n = sysconf(_SC_NPROCESSORS_ONLN);
	return (n > 0) ? (int) n : 1;
}

// Pin the calling process to the given CPU core.
static void
pin_to_cpu(int cpu)
{
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu, &cpuset);
	sched_setaffinity(0, sizeof(cpuset), &cpuset);
}

void
start_driver(const DataFileManager &inputFiles, char *szInDir,
		TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
		INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 iSeed,
		char *szBHaddr, int iBHlistenPort, int iUsers, int iPacingDelay,
		char *outputDirectory, int iSleep, int iTestDuration)
{
	CLogFormatTab fmt;
	char filename[iMaxPath + 1];

	// Initialize DM SUT interface and CDM for Trade-Cleanup.
	snprintf(filename, iMaxPath, "%s/Driver.log", outputDirectory);
	CEGenLogger *pLog
			= new CEGenLogger(eDriverEGenLoader, 0, filename, &fmt);

	CDMSUT *pCDMSUT = new CDMSUT(outputDirectory, szBHaddr, iBHlistenPort);

	pid_t main_pid = getpid();
	CDM *pCDM;
	if (iSeed == 0) {
		pCDM = new CDM(pCDMSUT, pLog, inputFiles, iConfiguredCustomerCount,
				iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades,
				main_pid);
	} else {
		pCDM = new CDM(pCDMSUT, pLog, inputFiles, iConfiguredCustomerCount,
				iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades,
				main_pid, iSeed);
	}

	// Run Trade-Cleanup before starting the test (same as threaded driver).
	cout << endl
		 << "Running Trade-Cleanup transaction before starting the test..."
		 << endl;
	pCDM->DoCleanupTxn();
	cout << "Trade-Cleanup transaction completed." << endl << endl;

	// Determine the number of worker processes.
	int nprocs = get_num_cpus();
	if (nprocs > iUsers)
		nprocs = iUsers; // no point forking more processes than users

	cout << "Forking " << nprocs << " worker processes for " << iUsers
		 << " simulated customers." << endl;

	// Calculate stop time.  Each worker staggers its customer connections
	// via nanosleep between CCustomer creation.  The largest partition
	// determines overall ramp-up duration.
	int customers_per_proc_max
			= iUsers / nprocs + (iUsers % nprocs > 0 ? 1 : 0);
	int ramp_up_seconds
			= (int) ((double) iSleep / 1000.0
					* (double) customers_per_proc_max);
	int stop_time = (int) time(NULL) + iTestDuration + ramp_up_seconds;

	CDateTime dtAux;
	cout << "Test is starting at " << dtAux.ToStr(02) << endl
		 << "Estimated duration of ramp-up: " << ramp_up_seconds << " seconds"
		 << endl;

	dtAux.AddMinutes((iTestDuration + ramp_up_seconds) / 60);
	cout << "Estimated end time " << dtAux.ToStr(02) << endl;

	double pacing_sec = (double) iPacingDelay / 1000.0;

	pid_t *child_pids = new pid_t[nprocs];
	int children_forked = 0;

	for (int p = 0; p < nprocs; p++) {
		// Determine this process's customer range.
		// Customers are numbered 1..iUsers.
		int customers_per_proc = iUsers / nprocs;
		int remainder = iUsers % nprocs;

		int start_uid = p * customers_per_proc + (p < remainder ? p : remainder)
				+ 1;
		int count = customers_per_proc + (p < remainder ? 1 : 0);

		pid_t pid = fork();

		if (pid < 0) {
			cerr << "fork() failed for worker " << p << endl;
			break;
		}

		if (pid == 0) {
			// ---- Child process ----

			pin_to_cpu(p % get_num_cpus());

			struct ev_loop *loop = ev_default_loop(0);

			// Load input files for this process (each process gets its own
			// copy, avoiding any shared-memory issues across fork).
			const DataFileManager procInputFiles(szInDir,
					iConfiguredCustomerCount, iActiveCustomerCount,
					TPCE::DataFileManager::IMMEDIATE_LOAD);

			// Allocate customer contexts for this process's partition.
			struct customer_context *contexts
					= new struct customer_context[count];

			// Stagger customer creation: sleep between each CCustomer
			// construction to avoid opening all sockets to BrokerageHouse
			// simultaneously.  This mirrors the threaded driver's ramp-up
			// sleep between pthread_create calls.
			struct timespec ts, rem;
			ts.tv_sec = (time_t) (iSleep / 1000);
			ts.tv_nsec = (long) (iSleep % 1000) * 1000000;

			for (int i = 0; i < count; i++) {
				int uid = start_uid + i;

				contexts[i].customer = new CCustomer(procInputFiles, szInDir,
						iConfiguredCustomerCount, iActiveCustomerCount,
						iScaleFactor, iDaysOfInitialTrades, iSeed, szBHaddr,
						iBHlistenPort, (UINT32) uid, iPacingDelay,
						outputDirectory);

				contexts[i].stop_time = stop_time;

				// All customers start immediately (initial delay = 0);
				// the ramp-up stagger comes from the nanosleep between
				// CCustomer creation above, not from timer delay.
				ev_timer_init(&contexts[i].pacing, pacing_cb, 0.0,
						pacing_sec);
				ev_timer_start(loop, &contexts[i].pacing);

				// Sleep between customer connections.
				if (i < count - 1) {
					while (nanosleep(&ts, &rem) == -1) {
						if (errno == EINTR) {
							memcpy(&ts, &rem, sizeof(struct timespec));
						} else {
							break;
						}
					}
				}
			}

			// Data Maintenance runs in the first worker process only.
			struct dm_context dm_ctx;
			memset(&dm_ctx, 0, sizeof(dm_ctx));

			if (p == 0) {
				// Re-create DM objects in the child process so we have our
				// own socket to BrokerageHouse (the parent's socket fd
				// should not be shared).
				CDMSUT *childDMSUT = new CDMSUT(
						outputDirectory, szBHaddr, iBHlistenPort);
				pid_t child_pid = getpid();
				CDM *childDM;
				if (iSeed == 0) {
					childDM = new CDM(childDMSUT, pLog, procInputFiles,
							iConfiguredCustomerCount, iActiveCustomerCount,
							iScaleFactor, iDaysOfInitialTrades, child_pid);
				} else {
					childDM = new CDM(childDMSUT, pLog, procInputFiles,
							iConfiguredCustomerCount, iActiveCustomerCount,
							iScaleFactor, iDaysOfInitialTrades, child_pid,
							iSeed);
				}

				dm_ctx.dm = childDM;
				dm_ctx.dm_sut = childDMSUT;
				dm_ctx.stop_time = stop_time;

				// DM fires immediately (first transaction at t=0), then
				// the callback re-arms with a 60-second delay.
				ev_timer_init(&dm_ctx.dm_timer, dm_timer_cb, 0.0, 0.0);
				ev_timer_start(loop, &dm_ctx.dm_timer);

				cout << ">> Data-Maintenance started in worker 0." << endl;
			}

			// Write the START marker after all customers in this worker
			// have been created and connected.  The measurement window
			// begins after the last worker finishes ramp-up.
			{
				char mix_filename[iMaxPath + 1];
				snprintf(mix_filename, iMaxPath, "%s/%s", outputDirectory,
						CE_MIX_LOG_NAME);
				ofstream fMix(mix_filename, ios::out | ios::app);
				fMix << (int) time(NULL) << ",START,,," << getpid() << endl;
				fMix.flush();
				fMix.close();
			}

			// Run the event loop until all timers have stopped (test
			// duration expired for every customer and DM).
			ev_run(loop, 0);

			// Cleanup.
			for (int i = 0; i < count; i++) {
				delete contexts[i].customer;
			}
			delete[] contexts;

			if (p == 0) {
				delete dm_ctx.dm;
				delete dm_ctx.dm_sut;
			}

			_exit(0);
		}

		// ---- Parent process ----
		child_pids[p] = pid;
		children_forked++;
	}

	cout << ">> All workers forked.  Waiting for test to complete." << endl;

	// Wait for all child processes to exit.
	for (int i = 0; i < children_forked; i++) {
		int status;
		waitpid(child_pids[i], &status, 0);
		if (WIFEXITED(status)) {
			cout << "Worker " << i << " (pid " << child_pids[i]
				 << ") exited with status " << WEXITSTATUS(status) << endl;
		}
	}

	delete[] child_pids;
	delete pCDM;
	delete pCDMSUT;
	delete pLog;

	cout << "Test completed." << endl;
}
