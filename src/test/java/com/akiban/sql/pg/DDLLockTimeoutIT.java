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

package com.akiban.sql.pg;

import com.akiban.server.error.ErrorCode;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.session.Session;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;
import static junit.framework.Assert.*;

public class DDLLockTimeoutIT extends PostgresServerITBase
{
    static enum State { 
        SET_TIMEOUT, TIMEOUT_SET, LOCK_DDL, DDL_LOCKED, QUERY, TIMEOUT, UNLOCK_DDL 
    };

    volatile State state;

    synchronized void waitForState(State newState) throws InterruptedException {
        while (state != newState) {
            wait();
        }
    }

    synchronized void setState(State state) {
        this.state = state;
        notifyAll();
    }


    @Test
    public void test() throws Exception {
        if(!DXLReadWriteLockHook.only().isDDLLockEnabled()) {
            return;
        }
        Thread queryThread = new QueryThread();
        Thread ddlThread = new DDLThread();
        queryThread.start();
        ddlThread.start();
        setState(State.SET_TIMEOUT);
        waitForState(State.TIMEOUT_SET);
        setState(State.LOCK_DDL);
        waitForState(State.DDL_LOCKED);
        setState(State.QUERY);
        waitForState(State.TIMEOUT);
        setState(State.UNLOCK_DDL);
        queryThread.join();
        ddlThread.join();
    }

    class QueryThread extends Thread {
        @Override
        public void run() {
            try {
                Statement statement = getConnection().createStatement();
                waitForState(DDLLockTimeoutIT.State.SET_TIMEOUT);
                statement.executeUpdate("SET queryTimeoutSec = '1'");
                setState(DDLLockTimeoutIT.State.TIMEOUT_SET);
                waitForState(DDLLockTimeoutIT.State.QUERY);
                try {
                    ResultSet rs = statement.executeQuery("SELECT 2+2");
                    fail("Query did not time out");
                }
                catch (SQLException ex) {
                    if (!ex.getSQLState().equals(ErrorCode.QUERY_TIMEOUT.getFormattedValue())) {
                        throw ex;
                    }
                }
                setState(DDLLockTimeoutIT.State.TIMEOUT);
                forgetConnection();
            }
            catch (Exception ex) {
                fail(ex.getMessage());
            }
        }
    }

    class DDLThread extends Thread {
        @Override
        public void run() {
            try {
                Session session = createNewSession();
                waitForState(DDLLockTimeoutIT.State.LOCK_DDL);
                DXLReadWriteLockHook.only().lock(session, DXLFunction.UNSPECIFIED_DDL_WRITE, -1);
                setState(DDLLockTimeoutIT.State.DDL_LOCKED);
                waitForState(DDLLockTimeoutIT.State.UNLOCK_DDL);
                DXLReadWriteLockHook.only().unlock(session, DXLFunction.UNSPECIFIED_DDL_WRITE);
            }
            catch (Exception ex) {
                fail(ex.getMessage());
            }
        }
    }

}
