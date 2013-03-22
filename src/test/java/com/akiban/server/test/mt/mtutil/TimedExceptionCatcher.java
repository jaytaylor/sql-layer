
package com.akiban.server.test.mt.mtutil;

import com.akiban.server.service.session.Session;

public abstract class TimedExceptionCatcher extends TimedCallable<Throwable> {

    protected abstract void doOrThrow(TimePoints timePoints, Session session) throws Exception;

    @Override
    protected final Throwable doCall(TimePoints timePoints, Session session) throws Exception {
        Throwable t = null;
        try {
            doOrThrow(timePoints, session);
        } catch (Throwable caught) {
            t = caught;
            handleCaught(timePoints, session, t);
        }
        return t;
    }

    protected void handleCaught(TimePoints timePoints, Session session, Throwable t) {
        // nothing
    }

    public static <T extends Throwable> void throwIfThrown(TimedResult<T> timedResult) throws T {
        T throwable = timedResult.getItem();
        if (throwable != null) {
            throw throwable;
        }
    }
}
