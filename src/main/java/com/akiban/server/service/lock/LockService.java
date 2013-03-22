
package com.akiban.server.service.lock;

import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;

public interface LockService extends Service {
    enum Mode { SHARED, EXCLUSIVE }

    boolean hasAnyClaims(Session session, Mode mode);
    boolean isTableClaimed(Session session, Mode mode, int tableID);

    void claimTable(Session session, Mode mode, int tableID);
    void claimTableInterruptible(Session session, Mode mode, int tableID) throws InterruptedException;

    boolean tryClaimTable(Session session, Mode mode, int tableID);
    boolean tryClaimTableMillis(Session session, Mode mode, int tableID, long milliseconds) throws InterruptedException;
    boolean tryClaimTableNanos(Session session, Mode mode, int tableID, long nanoseconds) throws InterruptedException;

    void releaseTable(Session session, Mode mode, int tableID);
}
