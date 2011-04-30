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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class SessionServiceImpl implements SessionService, Service<SessionService>, SessionEventListener, JmxManageable {
    private static Logger LOG = LoggerFactory.getLogger(SessionServiceImpl.class);

    private final Object LOCK = new Object();
    private final AtomicLong sessionsCreated = new AtomicLong();
    private final AtomicLong sessionsGCed = new AtomicLong();
    private final AtomicLong sessionsClosed = new AtomicLong();
    private final List<WeakReference<Session>> weakSessionsList = new ArrayList<WeakReference<Session>>();

    @Override
    public Session createSession() {
        Session session = new Session(this);
        WeakReference<Session> sessionRef = new WeakReference<Session>(session);
        sessionsCreated.incrementAndGet();
        synchronized (LOCK) {
            weakSessionsList.add(sessionRef);
        }
        return session;
    }

    @Override
    public long countSessionsCreated() {
        return sessionsCreated.get();
    }

    @Override
    public long countSessionsGCed() {
        return sessionsGCed.addAndGet( removeGCedReferences() );
    }

    @Override
    public void sessionClosing() {
        sessionsClosed.incrementAndGet();
    }

    @Override
    public long countSessionsClosed() {
        return sessionsClosed.get();
    }

    private int removeGCedReferences() {
        final int size;
        int removed = 0;
        synchronized (LOCK) {
            size = weakSessionsList.size();
            Iterator<WeakReference<Session>> iterator = weakSessionsList.iterator();
            while (iterator.hasNext()) {
                WeakReference<Session> weakReference = iterator.next();
                if (weakReference.get() == null) {
                    iterator.remove();
                    ++removed;
                }
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Removed " + removed + " of " + size + " session" + (size == 1 ? "s." : "."));
        }
        return removed;
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Sessions", new SessionServiceMXBean() {
            @Override
            public long getCreated() {
                return countSessionsCreated();
            }

            @Override
            public long getGCed() {
                return countSessionsGCed();
            }

            @Override
            public long getClosed() {
                return countSessionsClosed();
            }
        }, SessionServiceMXBean.class);
    }

    @Override
    public SessionService cast() {
        return this;
    }

    @Override
    public Class<SessionService> castClass() {
        return SessionService.class;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public void crash() throws Exception {
    }
}
