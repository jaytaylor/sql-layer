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

package com.akiban.server.service.session;

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import java.util.concurrent.atomic.AtomicLong;

public final class SessionServiceImpl implements SessionService, Service<SessionService>, SessionEventListener, JmxManageable {

    private final AtomicLong sessionsCreated = new AtomicLong();
    private final AtomicLong sessionsClosed = new AtomicLong();

    // SessionService interface

    @Override
    public Session createSession() {
        Session session = new Session(this);
        sessionsCreated.incrementAndGet();
        return session;
    }

    @Override
    public long countSessionsCreated() {
        return sessionsCreated.get();
    }

    @Override
    public long countSessionsClosed() {
        return sessionsClosed.get();
    }

    // SessionEventListener interface

    @Override
    public void sessionClosing(Session session) {
        sessionsClosed.incrementAndGet();
    }

    // JmxManageable interface

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Sessions", new SessionServiceMXBean() {
            @Override
            public long getCreated() {
                return countSessionsCreated();
            }

            @Override
            public long getClosed() {
                return countSessionsClosed();
            }
        }, SessionServiceMXBean.class);
    }

    // Service<SessionService> interface

    @Override
    public SessionService cast() {
        return this;
    }

    @Override
    public Class<SessionService> castClass() {
        return SessionService.class;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }
}
