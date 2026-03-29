/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Event-driven Driver using libev, replacing the thread-per-customer model.
 * Based on the dbt-2 driver2 pattern.
 */

#ifndef DRIVER2_H
#define DRIVER2_H

#include <ev.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "EGenLogFormatterTab.h"
#include "EGenLogger.h"
#include "DMSUT.h"
#include "Customer.h"

using namespace TPCE;

// Per-customer state managed by the event loop.
// The ev_timer must be accessible so the callback can recover the parent
// struct via container_of / offsetof.
struct customer_context
{
	ev_timer pacing; // pacing delay timer watcher
	CCustomer *customer; // owns CCE, CCESUT (persistent socket to BH)
	int stop_time; // when to stop submitting transactions
};

// Data Maintenance context -- fires once per minute in one worker process.
struct dm_context
{
	ev_timer dm_timer; // periodic timer watcher
	CDM *dm; // Data Maintenance transaction generator
	CDMSUT *dm_sut; // DM SUT interface (socket to BH)
	int stop_time;
};

// Launch the event-driven driver.
// Forks worker processes, partitions customers, runs ev_timer-based loops.
void start_driver(const DataFileManager &inputFiles, char *szInDir,
		TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
		INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 iSeed,
		char *szBHaddr, int iBHlistenPort, int iUsers, int iPacingDelay,
		char *outputDirectory, int iSleep, int iTestDuration);

#endif // DRIVER2_H
