
package com.akiban.server.service.session;

public interface SessionService extends SessionFactory {
    long countSessionsCreated();
    long countSessionsClosed();
}
