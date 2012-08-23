/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
