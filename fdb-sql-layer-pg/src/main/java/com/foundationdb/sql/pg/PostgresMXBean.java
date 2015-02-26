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

package com.foundationdb.sql.pg;

import java.util.Date;
import java.util.Set;

public interface PostgresMXBean {
        
    /**
     * @param sessionId connection ID
     * @return text of current/last query this connection executed
     */
    String getSqlString(int sessionId);
    
    /**
     * @param sessionId connection ID
     * @return client's IP address
     */
    String getRemoteAddress(int sessionId);

    //int getStatementCacheCapacity();
    //void setStatementCacheCapacity(int capacity);
    //int getStatementCacheHits();
    //int getStatementCacheMisses();
    //void resetStatementCache();
    
    Set<Integer> getCurrentSessions();

    /*
     * information on individual sessions being monitored
     */
    Date getStartTime(int sessionId);
    long getProcessingTime(int sessionId);
    /* below are only for the last statement executed */
    long getEventTime(int sessionId, String eventName);
    long getTotalEventTime(int sessionId, String eventName);

    /*
     * Returns the uptime of the Postgres Server in nanoseconds.
     */
    long getUptime();

    /** Cancel any running query for the given connection. */
    void cancelQuery(int sessionId);
    /** Kill the given connection. */
    void killConnection(int sessionId);
}
