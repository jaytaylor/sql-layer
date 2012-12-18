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

package com.akiban.sql.server;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContextBase;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.error.QueryTimedOutException;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.session.Session;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.io.IOException;

public class ServerQueryContext<T extends ServerSession> extends QueryContextBase
{
    private T server;

    public ServerQueryContext(T server) {
        this.server = server;
    }

    public T getServer() {
        return server;
    }

    @Override
    public StoreAdapter getStore() {
        return server.getStore();
    }

    @Override
    public StoreAdapter getStore(UserTable table) {
        return server.getStore(table);
    }

    @Override
    public Session getSession() {
        return server.getSession();
    }

    @Override
    public String getCurrentUser() {
        return getSessionUser();
    }

    @Override
    public String getSessionUser() {
        return server.getProperty("user");
    }

    @Override
    public String getCurrentSchema() {
        return server.getDefaultSchemaName();
    }

    @Override
    public int getSessionId() {
        return server.getSessionMonitor().getSessionId();
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        try {
            server.notifyClient(level, errorCode, message);
        }
        catch (IOException ex) {
        }
    }

    @Override
    public long getQueryTimeoutSec() {
        return server.getQueryTimeoutSec();
    }

    @Override
    public long sequenceNextValue(TableName sequenceName) {
        return server.getStore().sequenceNextValue(sequenceName);
    }

    @Override
    public long sequenceCurrentValue(TableName sequenceName) {
        return server.getStore().sequenceCurrentValue(sequenceName);
    }

    public void lock(DXLFunction operationType) {
        long timeout = 0;       // No timeout.
        long queryTimeoutSec = getQueryTimeoutSec();
        if (queryTimeoutSec >= 0) {
            long runningTimeMsec = System.currentTimeMillis() - getStartTime();
            timeout = queryTimeoutSec * 1000 - runningTimeMsec;
            if (timeout <= 0) {
                // Already past time.
                throw new QueryTimedOutException(runningTimeMsec);
            }
        }
        try {
            boolean locked = DXLReadWriteLockHook.only().lock(getSession(), operationType, timeout);
            if (!locked) {
                throw new QueryTimedOutException(System.currentTimeMillis() - getStartTime());
            }
        }
        catch (InterruptedException ex) {
            throw new QueryCanceledException(getSession());
        }
    }

    public void unlock(DXLFunction operationType) {
        DXLReadWriteLockHook.only().unlock(getSession(), operationType);
    }

}
