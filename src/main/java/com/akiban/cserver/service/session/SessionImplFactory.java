package com.akiban.cserver.service.session;

public final class SessionImplFactory implements SessionFactory {
    @Override
    public Session createSession() {
        return new SessionImpl();
    }
}
