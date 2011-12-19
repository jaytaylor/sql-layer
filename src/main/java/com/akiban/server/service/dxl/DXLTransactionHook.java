package com.akiban.server.service.dxl;

import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

public class DXLTransactionHook implements DXLFunctionsHook {
    private static final Session.StackKey<Boolean> AUTO_TRX_CLOSE = Session.StackKey.stackNamed("AUTO_TRX_CLOSE");
    private final TreeService treeService;

    public DXLTransactionHook(TreeService treeService) {
        this.treeService = treeService;
    }

    @Override
    public void hookFunctionIn(Session session, DXLFunction function) {
        Transaction trx = treeService.getTransaction(session);
        if(!trx.isActive()) {
            try {
                trx.begin();
                session.push(AUTO_TRX_CLOSE, Boolean.TRUE);
            } catch(PersistitException e) {
                throw new PersistItErrorException(e);
            }
        }
        else {
            session.push(AUTO_TRX_CLOSE, Boolean.FALSE);
        }
    }

    @Override
    public void hookFunctionCatch(Session session, DXLFunction function, Throwable throwable) {
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
        Boolean doAuto = session.pop(AUTO_TRX_CLOSE);
        if(doAuto != null && doAuto) {
            Transaction trx = treeService.getTransaction(session);
            if(trx.isActive()) {
                try {
                    trx.commit();
                } catch(PersistitException e) {
                    throw new PersistItErrorException(e);
                }
                finally {
                    trx.end();
                }
            }
        }
    }
}
