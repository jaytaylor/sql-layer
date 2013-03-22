
package com.akiban.server.service.dxl;

import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;

public class DXLTransactionHook implements DXLFunctionsHook {
    private static final Session.StackKey<Boolean> AUTO_TRX_CLOSE = Session.StackKey.stackNamed("AUTO_TRX_CLOSE");
    private final TransactionService txnService;

    public DXLTransactionHook(TransactionService txnService) {
        this.txnService = txnService;
    }

    @Override
    public void hookFunctionIn(Session session, DXLFunction function) {
        if(!needsTransaction(function)) {
            return;
        }
        if(!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            session.push(AUTO_TRX_CLOSE, Boolean.TRUE);
        } else {
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
            txnService.rollbackTransaction(session);
        }
    }

    @Override
    public void hookFunctionFinally(Session session, DXLFunction function, Throwable throwable) {
        if(!needsTransaction(function)) {
            return;
        }
        Boolean doAuto = session.pop(AUTO_TRX_CLOSE);
        if(doAuto != null && doAuto) {
            txnService.commitTransaction(session);
        }
    }
    
    private static boolean needsTransaction(DXLFunction function) {
        switch(function) {
            // DDL modifying existing table(s), locking and manual transaction handling needed
            case DROP_TABLE:
            case ALTER_TABLE:
            case DROP_SCHEMA:
            case DROP_GROUP:
            case CREATE_INDEXES:
            case DROP_INDEXES:
                return false;

            // DDL changing AIS but does not scan or modify existing table (locking not needed)
            case RENAME_TABLE:
            case CREATE_TABLE:
            case CREATE_VIEW:
            case DROP_VIEW:
            case CREATE_SEQUENCE:
            case DROP_SEQUENCE:
            case CREATE_ROUTINE:
            case DROP_ROUTINE:
            case CREATE_SQLJ_JAR:
            case REPLACE_SQLJ_JAR:
            case DROP_SQLJ_JAR:
                return true;

            // Remaining methods on DDL interface, querying only
            case GET_DDLS:
            case UPDATE_TABLE_STATISTICS:
            case CHECK_AND_FIX_INDEXES:
            case GET_AIS:
            case GET_TABLE_ID:
            case GET_TABLE_BY_ID:
            case GET_TABLE_BY_NAME:
            case GET_USER_TABLE_BY_NAME:
            case GET_USER_TABLE_BY_ID:
            case GET_ROWDEF:
            case GET_SCHEMA_ID:
            case GET_SCHEMA_TIMESTAMP:
                return true;

            // DML that looks at AIS
            case GET_TABLE_STATISTICS:
            case SCAN_SOME:
            case WRITE_ROW:
            case WRITE_ROWS:
            case DELETE_ROW:
            case UPDATE_ROW:
            case TRUNCATE_TABLE:
            case CONVERT_NEW_ROW:
            case CONVERT_ROW_DATA:
            case CONVERT_ROW_DATAS:
            case OPEN_CURSOR:
                return true;

            // Remaining DML
            case GET_CURSOR_STATE:
            case CLOSE_CURSOR:
            case GET_CURSORS:
                return false;
        }

        throw new IllegalArgumentException("Unexpected function for hook " + function);
    }
}
