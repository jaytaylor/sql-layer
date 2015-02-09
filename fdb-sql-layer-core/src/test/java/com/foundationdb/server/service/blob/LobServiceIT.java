/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

import com.foundationdb.server.error.*;
import com.foundationdb.server.service.transaction.*;
import com.foundationdb.server.store.*;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.Transaction;
import java.util.UUID;
import java.util.Random;

import org.junit.*;


public class LobServiceIT extends ITBase {
    private LobService ls;
    private String idA;
    private String idB;
    private final byte[] data = "foo".getBytes();
    
    @Test
    public void utilizeLobService() {
        // registration
        Assert.assertNotNull(ls);

        // blob creation
        String id = UUID.randomUUID().toString();
        Transaction tr = getTransaction();
        ls.createNewLob(tr, id);
        commit();

        ls.appendBlob(getTransaction(), id, data);
        commit();
        
        Assert.assertEquals(ls.sizeBlob(getTransaction(), id), new Long(data.length).longValue());
        commit();
        
        // blob transfer to new address
        String newPath = UUID.randomUUID().toString();
        ls.moveLob(getTransaction(), id, newPath);
        commit();
        
        // data retrieval
        byte[] output = ls.readBlob(getTransaction(), newPath, 0, data.length);
        commit();
        Assert.assertArrayEquals(data, output);
        output = ls.readBlob(getTransaction(), newPath);
        commit();
        Assert.assertArrayEquals(data, output);
        
        ls.truncateBlob(getTransaction(), newPath, 2L);
        commit();
        Assert.assertEquals(ls.sizeBlob(getTransaction(), newPath), 2L);
        commit();
        
        
        ls.truncateBlob(getTransaction(), newPath, 0L);
        commit();
        
        Assert.assertEquals(ls.sizeBlob(getTransaction(), newPath), 0L);
        commit();
        ls.appendBlob(getTransaction(), newPath, data);
        commit();
        output = ls.readBlob(getTransaction(), newPath);
        commit();
        Assert.assertArrayEquals(data, output);        
        
        ls.deleteLob(getTransaction(), newPath);
        commit();
        Assert.assertFalse(ls.existsLob(getTransaction(), newPath));
        commit();
    }
    
    @Before
    public void setUp(){
        // registration
        this.ls = serviceManager().getServiceByClass(LobService.class);
        Assert.assertNotNull(ls);
        Transaction tr = getTransaction();
        idA = UUID.randomUUID().toString();
        idB = UUID.randomUUID().toString();
        ls.createNewLob(tr, idA);
        ls.createNewLob(tr, idB);
        ls.linkTableBlob(tr, idA, 1);
        commit();
    }
    
    @Test
    public void blobGarbageCollector() {
        // run collector
        ls.runLobGarbageCollector();
        
        // check cleaning
        Assert.assertTrue(ls.existsLob(getTransaction(), idA));
        Assert.assertFalse(ls.existsLob(getTransaction(), idB));
        commit();
    }
    
    
    private Transaction getTransaction() {
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
        ts.commitOrRetryTransaction(session());
        ts.rollbackTransactionIfOpen(session());
    }

    private byte[] generateBytes(int length) {
        byte[] inp = new byte[length];
        Random random = new Random();
        random.nextBytes(inp);
        return inp;
    }
}
