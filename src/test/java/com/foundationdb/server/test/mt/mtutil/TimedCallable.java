/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.test.mt.mtutil;

import com.foundationdb.server.service.ServiceManagerImpl;
import com.foundationdb.server.service.session.Session;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public abstract class TimedCallable<T> implements Callable<TimedResult<T>> {
    private final AtomicReference<TimePoints> timePointsReference = new AtomicReference<>();
    protected abstract T doCall(TimePoints timePoints, Session session) throws Exception;

    @Override
    public final TimedResult<T> call() throws Exception {
        TimePoints timePoints = new TimePoints();
        if (!timePointsReference.compareAndSet(null, timePoints)) {
            throw new RuntimeException("TimePoints already set!");
        }
        T result = doCall(timePoints, ServiceManagerImpl.newSession());
        return new TimedResult<>(result, timePoints);
    }
    
    public TimePoints getTimePoints() {
        return timePointsReference.get();
    }
}
