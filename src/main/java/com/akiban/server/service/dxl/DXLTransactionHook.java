/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
        if (session.isEmpty(AFTER_COMMIT_RUNNABLES))
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
            case FORCE_GENERATION_UPDATE:
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
