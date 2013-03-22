
package com.akiban.server.service.session;

public final class TestSessionFactory implements SessionFactory {

    private static final TestSessionFactory INSTANCE = new TestSessionFactory();

    public static SessionFactory get() {
        return INSTANCE;
    }

    private TestSessionFactory() {
        // private ctor
    }

    @Override
    public Session createSession() {
        return new Session(null);
    }
}
