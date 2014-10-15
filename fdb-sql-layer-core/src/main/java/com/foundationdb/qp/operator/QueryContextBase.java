/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.qp.operator;

import com.foundationdb.server.error.*;
import com.foundationdb.sql.server.ServerTransaction;

import java.util.Date;

public abstract class QueryContextBase implements QueryContext
{
    // startTimeMsec is used to control query timeouts.
    private final long startTimeMsec = System.currentTimeMillis();
    private long queryTimeoutMsec = Long.MAX_VALUE;

    /* QueryContext interface */

    @Override
    public Date getCurrentDate() {
        return new Date();
    }

    @Override
    public String getSystemUser() {
        return System.getProperty("user.name");
    }

    @Override
    public long getStartTime() {
        return startTimeMsec;
    }

    @Override
    public void warnClient(InvalidOperationException exception) {
        notifyClient(NotificationLevel.WARNING, exception.getCode(), exception.getShortMessage());
    }

    @Override
    public long getQueryTimeoutMilli() {
        if (queryTimeoutMsec == Long.MAX_VALUE) {
            queryTimeoutMsec = getStore().getQueryTimeoutMilli();
        }
        return queryTimeoutMsec;
    }

    @Override
    public void checkQueryCancelation() {
        if (getSession().isCurrentQueryCanceled()) {
            throw new QueryCanceledException(getSession());
        }
        long queryTimeoutMilli = getQueryTimeoutMilli();
        if (queryTimeoutMilli >= 0) {
            long runningTimeMsec = System.currentTimeMillis() - getStartTime();
            if (runningTimeMsec > queryTimeoutMilli) {
                throw new QueryTimedOutException(runningTimeMsec);
            }
        }
    }

    @Override
    public ServerTransaction.PeriodicallyCommit getTransactionPeriodicallyCommit() {
        return ServerTransaction.PeriodicallyCommit.OFF;
    }

    @Override
    public QueryBindings createBindings() {
        return new SparseArrayQueryBindings();
    }
}
