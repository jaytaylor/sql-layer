/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store;

/**
 * Control how a scan of an index / group interacts with transactions.
 */
public class FDBScanTransactionOptions
{
    public static final FDBScanTransactionOptions NORMAL = new FDBScanTransactionOptions();
    public static final FDBScanTransactionOptions SNAPSHOT = new FDBScanTransactionOptions(true);
   
    private final boolean snapshot;
    private final int commitAfterRows;
    private final long commitAfterMillis;
    private final long sleepAfterCommit;

    public FDBScanTransactionOptions() {
        this(false, -1, -1, -1);
    }

    public FDBScanTransactionOptions(boolean snapshot) {
        this(snapshot, -1, -1, -1);
    }

    public FDBScanTransactionOptions(int commitAfterRows, long commitAfterMillis) {
        this(false, commitAfterRows, commitAfterMillis, -1);
    }

    public FDBScanTransactionOptions(long commitAfterMillis, long sleepAfterCommit) {
        this(false, -1, commitAfterMillis, sleepAfterCommit);
    }

    public FDBScanTransactionOptions(boolean snapshot, int commitAfterRows,
                                     long commitAfterMillis, long sleepAfterCommit) {
        this.snapshot = snapshot;
        this.commitAfterRows = commitAfterRows;
        this.commitAfterMillis = commitAfterMillis;
        this.sleepAfterCommit = sleepAfterCommit;
    }

    /** Should scan use snapshot read to avoid generating conflicts? */
    public boolean isSnapshot() {
        return snapshot;
    }

    /** Should we ever commit in the middle of a scan? */
    public boolean isCommit() {
        return ((commitAfterRows > 0) ||
                (commitAfterMillis > 0));
    }

    /** Should scan commit after a number of rows have been traversed? */
    public int getCommitAfterRows() {
        return commitAfterRows;
    }

    public boolean shouldCommitAfterRows(int rows) {
        return ((commitAfterRows > 0) &&
                (rows >= commitAfterRows));
    }
    
    /** Should scan commit after some time has passed since the
     * beginning of the transaction? */
    public long getCommitAfterMillis() {
        return commitAfterMillis;
    }

    public boolean shouldCommitAfterMillis(long startTime) {
        if (commitAfterMillis <= 0) return false;
        long dt = System.currentTimeMillis() - startTime;
        return (dt >= commitAfterMillis);
    }

    /** Time to wait after committing while scanning. */
    public long getSleepAfterCommit() {
        return sleepAfterCommit;
    }

    public void maybeSleepAfterCommit() throws InterruptedException {
        if (sleepAfterCommit > 0) {
            Thread.sleep(sleepAfterCommit);
        }
    }
}
