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
