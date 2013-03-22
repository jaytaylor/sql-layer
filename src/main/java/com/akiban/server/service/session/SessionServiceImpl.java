
package com.akiban.server.service.session;

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import java.util.concurrent.atomic.AtomicLong;

public final class SessionServiceImpl implements SessionService, Service, SessionEventListener, JmxManageable {

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
    public void sessionClosing() {
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
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }
}
