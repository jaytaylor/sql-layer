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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.util.ArgumentValidation;

public final class SessionServiceImpl implements SessionService, JmxManageable, Service<SessionService>, SessionFactory
{
    private final SessionFactory factory;
    private final Map<SessionHandle, Session> sessions = new HashMap<SessionHandle, Session>();
    private final Set<SessionHandle> usedSessionHandles = new HashSet<SessionHandle>();
    private final Object INTERNAL_LOCK = new Object();

    private int sessionsCreated = 0;
    private int sessionsClosed = 0;
    
    public SessionServiceImpl()
    {
        this.factory = this;
    }
    
    public SessionServiceImpl(SessionFactory factory) {
        ArgumentValidation.notNull("SessionFactory", factory);
        this.factory = factory;
    }

    @Override
    public void start() throws Exception {
        // No-op
    }

    @Override
    public void stop() throws Exception {
        // No-op
    }
    
    @Override
    public void crash() throws Exception {
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
    public SessionHandle createSessionWithHandle()
    {
        SessionHandle handle = new DefaultSessionHandle();
        createSession(handle);
        return handle;
    }
    
    @Override
    public void createSession(SessionHandle sessionHandle) throws SessionException {
        ArgumentValidation.notNull("SessionHandle", sessionHandle);
        final Session created;
        try {
            created = factory.createSession();
        } catch (Exception e) {
            throw new SessionException(e);
        }
        if (created == null) {
            throw new SessionException("Factory created null Session: " + factory);
        }
        synchronized (INTERNAL_LOCK) {
            final Session oldSession = sessions.put(sessionHandle, created);
            if (oldSession != null) {
                sessions.put(sessionHandle, oldSession);
                throw new SessionException("Already have a session for that sessionHandle");
            }
            ++sessionsCreated;
        }
    }

    @Override
    public Session acquireSession(SessionHandle sessionHandle) throws SessionException {
        ArgumentValidation.notNull("SessionHandle", sessionHandle);
        final Session installed;
        synchronized (INTERNAL_LOCK) {
            installed = sessions.get(sessionHandle);
            if (installed == null) {
                throw new SessionException("Unknown session sessionHandle: " + sessionHandle);
            }
            if (!usedSessionHandles.add(sessionHandle)) {
                throw new SessionException(sessionHandle + " is already being used");
            }
        }
        return installed;
    }

    @Override
    public void releaseSession(SessionHandle sessionHandle) throws SessionException {
        ArgumentValidation.notNull("SessionHandle", sessionHandle);
        synchronized (INTERNAL_LOCK) {
            if(!usedSessionHandles.remove(sessionHandle)) {
                throw new SessionException(sessionHandle + " is not being used by this service");
            }
        }
    }

    @Override
    public void destroySession(SessionHandle sessionHandle) throws SessionException {
        ArgumentValidation.notNull("SessionHandle", sessionHandle);
        synchronized (INTERNAL_LOCK) {
            final Session closedSession = sessions.remove(sessionHandle);
            if (closedSession == null) {
                throw new SessionException(sessionHandle + " is not managed by this service");
            }
            usedSessionHandles.remove(sessionHandle);
            ++sessionsClosed;
        }
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Sessions", bean, SessionServiceMXBean.class);
    }

    /**
     * For unit testing.
     * @return the JMX bean
     */
    SessionServiceMXBean getBean() {
        return bean;
    }

    private final SessionServiceMXBean bean = new SessionServiceMXBean() {
        @Override
        public int getSessionsCreated() {
            synchronized (INTERNAL_LOCK) {
                return sessionsCreated;
            }
        }

        @Override
        public int getSessionsClosed() {
            synchronized (INTERNAL_LOCK) {
                return sessionsClosed;
            }
        }

        @Override
        public int getSessionsActive() {
            synchronized (INTERNAL_LOCK) {
                return usedSessionHandles.size();
            }
        }
    };

    private static class DefaultSessionHandle implements SessionHandle
    {
        // Empty class to create concrete handles.
    }

    @Override
    public Session createSession()
    {
        return new SessionImpl();
    }
}
