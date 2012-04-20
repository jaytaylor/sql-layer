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

package com.akiban.server.test.mt.mtutil;

import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public abstract class TimedCallable<T> implements Callable<TimedResult<T>> {
    private final AtomicReference<TimePoints> timePointsReference = new AtomicReference<TimePoints>();
    protected abstract T doCall(TimePoints timePoints, Session session) throws Exception;

    @Override
    public final TimedResult<T> call() throws Exception {
        TimePoints timePoints = new TimePoints();
        if (!timePointsReference.compareAndSet(null, timePoints)) {
            throw new RuntimeException("TimePoints already set!");
        }
        T result = doCall(timePoints, ServiceManagerImpl.newSession());
        return new TimedResult<T>(result, timePoints);
    }
    
    public TimePoints getTimePoints() {
        return timePointsReference.get();
    }
}
