/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.mt.mtutil;

import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;

import java.util.concurrent.Callable;

public abstract class TimedCallable<T> implements Callable<TimedResult<T>> {
    protected abstract T doCall(TimePoints timePoints, Session session) throws Exception;

    @Override
    public final TimedResult<T> call() throws Exception {
        TimePoints timePoints = new TimePoints();
        T result = doCall(timePoints, new SessionImpl());
        return new TimedResult<T>(result, timePoints);
    }
}
