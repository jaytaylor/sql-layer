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

package com.akiban.server.service.dxl;

import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.util.MultipleCauseException;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

public class DXLTransactionHook implements DXLFunctionsHook {
    private static final Session.StackKey<Runnable> AFTER_COMMIT_RUNNABLES
            = Session.StackKey.stackNamed("AFTER_COMMIT_RUNNABLES");
    private static final Session.StackKey<Boolean> AUTO_TRX_CLOSE = Session.StackKey.stackNamed("AUTO_TRX_CLOSE");
    private final TreeService treeService;

    public static void addCommitSuccessCallback(Session session, Runnable callback) {
        if (session.isEmpty(AUTO_TRX_CLOSE))
            throw new IllegalStateException("not within a transaction");
        session.push(AFTER_COMMIT_RUNNABLES, callback);
    }
    
    public DXLTransactionHook(TreeService treeService) {
        this.treeService = treeService;
    }

    @Override
    public void hookFunctionIn(Session session, DXLFunction function) {
        if(!needsTransaction(function)) {
            return;
        }
        Transaction trx = treeService.getTransaction(session);
        if(!trx.isActive()) {
            try {
                trx.begin();
                session.push(AUTO_TRX_CLOSE, Boolean.TRUE);
            } catch(PersistitException e) {
                throw new PersistitAdapterException(e);
            }
        }
        else {
            session.push(AUTO_TRX_CLOSE, Boolean.FALSE);
        }
    }

    @Override
    public void hookFunctionCatch(Session session, DXLFunction function, Throwable throwable) {
        if(!needsTransaction(function)) {
            return;
        }
        Boolean doAuto = session.pop(AUTO_TRX_CLOSE);
        if(doAuto != null && doAuto) {
            Transaction trx = treeService.getTransaction(session);
            if(trx.isActive()) {
                if(!trx.isRollbackPending()) {
                    trx.rollback();
                }
                trx.end();
            }
        }
    }

    @Override
    public void hookFunctionFinally(Session session, DXLFunction function, Throwable throwable) {
        if(!needsTransaction(function)) {
            return;
        }
        Boolean doAuto = session.pop(AUTO_TRX_CLOSE);
        if(doAuto != null && doAuto) {
            Transaction trx = treeService.getTransaction(session);
            if(trx.isActive()) {
                try {
                    trx.commit();
                } catch(PersistitException e) {
                    throw new PersistitAdapterException(e);
                }
                finally {
                    trx.end();
                }
                if (throwable == null) {
                    RuntimeException callbackExceptions = null;
                    for (Runnable callback; (callback = session.pop(AFTER_COMMIT_RUNNABLES)) != null; ) {
                        try {
                            callback.run();
                        }
                        catch (RuntimeException e) {
                            callbackExceptions = MultipleCauseException.combine(callbackExceptions, e);
                        }
                    }
                    if (callbackExceptions != null)
                        throw callbackExceptions;
                }
            }
        }
    }
    
    private static boolean needsTransaction(DXLFunction function) {
        switch(function) {
            case CREATE_TABLE:
            case RENAME_TABLE:
            case DROP_TABLE:
            case ALTER_TABLE:
            case CREATE_VIEW:
            case DROP_VIEW:
            case DROP_SCHEMA:
            case DROP_GROUP:
            case CREATE_INDEXES:
            case DROP_INDEXES:
            case GET_DDLS:
            case GET_TABLE_STATISTICS:
            case SCAN_SOME:
            case WRITE_ROW:
            case DELETE_ROW:
            case UPDATE_ROW:
            case TRUNCATE_TABLE:
            case UPDATE_TABLE_STATISTICS:
            case CHECK_AND_FIX_INDEXES:
            case CREATE_SEQUENCE:
            case DROP_SEQUENCE:
            case CREATE_PROCEDURE:
            case DROP_PROCEDURE:
                return true;

            case GET_AIS:
            case GET_TABLE_ID:
            case GET_TABLE_BY_ID:
            case GET_TABLE_BY_NAME:
            case GET_USER_TABLE_BY_NAME:
            case GET_USER_TABLE_BY_ID:
            case GET_ROWDEF:
            case GET_SCHEMA_ID:
            case GET_SCHEMA_TIMESTAMP:
            case OPEN_CURSOR:
            case GET_CURSOR_STATE:
            case CLOSE_CURSOR:
            case GET_CURSORS:
            case CONVERT_NEW_ROW:
            case CONVERT_ROW_DATA:
            case CONVERT_ROW_DATAS:
                return false;
        }

        throw new IllegalArgumentException("Unexpected function for hook " + function);
    }
}
