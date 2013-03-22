
package com.akiban.server.test.mt.mtutil;

import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public abstract class TimedCallable<T> implements Callable<TimedResult<T>> {
    private final AtomicReference<TimePoints> timePointsReference = new AtomicReference<>();
    protected abstract T doCall(TimePoints timePoints, Session session) throws Exception;

    @Override
    public final TimedResult<T> call() throws Exception {
        TimePoints timePoints = new TimePoints();
        if (!timePointsReference.compareAndSet(null, timePoints)) {
            throw new RuntimeException("TimePoints already set!");
        }
        T result = doCall(timePoints, ServiceManagerImpl.newSession());
        return new TimedResult<>(result, timePoints);
    }
    
    public TimePoints getTimePoints() {
        return timePointsReference.get();
    }
}
