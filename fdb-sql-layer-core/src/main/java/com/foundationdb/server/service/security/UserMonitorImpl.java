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
package com.foundationdb.server.service.security;

import java.util.concurrent.atomic.AtomicLong;

import com.foundationdb.server.service.monitor.UserMonitor;

public class UserMonitorImpl implements UserMonitor {
    private final String userName;
    protected final AtomicLong count = new AtomicLong();

    public UserMonitorImpl (String name) {
        this.userName = name;
    }
    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public long getStatementCount() {
        return count.get();
    }

    @Override
    public void statementRun() {
        count.incrementAndGet();
    }
    
    @Override
    public long getNonIdleTimeNanos() {
        // TODO Auto-generated method stub
        return 0;
    }

}
