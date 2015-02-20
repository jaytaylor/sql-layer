/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.blob;

import com.foundationdb.server.service.transaction.*;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.Transaction;
import java.util.UUID;

import org.junit.*;


public class LobServiceIT extends ITBase {
    private LobService ls;
    private UUID idA;
    private UUID idB;
    private final byte[] data = "foo".getBytes();
    
    @Test
    public void utilizeLobService() {
        // registration
        Assert.assertNotNull(ls);

        // blob creation
        UUID id = UUID.randomUUID();
        getOrBeginTransaction();
        ls.createNewLob(session(), id);
        ls.appendBlob(session(), id, data);
        Assert.assertEquals(ls.sizeBlob(session(), id), new Long(data.length).longValue());
        
        // data retrieval
        byte[] output = ls.readBlob(session(), id, 0, data.length);
        Assert.assertArrayEquals(data, output);
        output = ls.readBlob(session(), id);
        Assert.assertArrayEquals(data, output);
        
        ls.truncateBlob(session(), id, 2L);
        Assert.assertEquals(ls.sizeBlob(session(), id), 2L);
        ls.truncateBlob(session(), id, 0L);
        Assert.assertEquals(ls.sizeBlob(session(), id), 0L);
        ls.appendBlob(session(), id, data);
        output = ls.readBlob(session(), id);
        Assert.assertArrayEquals(data, output);        
        
        ls.deleteLob(session(), id);
        Assert.assertFalse(ls.existsLob(session(), id));
        ls.createNewLob(session(), id);
        commit();
    }
    
    @Before
    public void setUp(){
        // registration
        this.ls = serviceManager().getServiceByClass(LobService.class);
        Assert.assertNotNull(ls);
        getOrBeginTransaction();
        idA = UUID.randomUUID();
        idB = UUID.randomUUID();
        ls.createNewLob(session(), idA);
        ls.createNewLob(session(), idB);
        ls.linkTableBlob(session(), idA, 1);
        commit();
    }
    
    @Test
    public void checkBlobCleanUp() {
        getOrBeginTransaction();
        Assert.assertTrue(ls.existsLob(session(), idA));
        Assert.assertFalse(ls.existsLob(session(), idB));
        commit();
    }
    
    
    private Transaction getOrBeginTransaction() {
        TransactionService txnService = txnService();
        if (txnService instanceof FDBTransactionService) {
            if ( txnService.isTransactionActive(session())) {
                return ((FDBTransactionService) txnService).getTransaction(session()).getTransaction();
            } else {
                txnService.beginTransaction(session());
                return ((FDBTransactionService) txnService).getTransaction(session()).getTransaction();
            }
        }
        else 
            return null;
    }
    
    private void commit() {
        TransactionService ts = txnService();
        ts.commitTransaction(session());
        ts.rollbackTransactionIfOpen(session());
    }
}
