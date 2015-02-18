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
import java.util.Random;

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
        getTransaction();
        ls.createNewLob(session(), id);
        commit();

        getTransaction();
        ls.appendBlob(session(), id, data);
        commit();
        
        getTransaction();
        Assert.assertEquals(ls.sizeBlob(session(), id), new Long(data.length).longValue());
        commit();
        
        // blob transfer to new address
        UUID newPath = UUID.randomUUID();
        getTransaction();
        ls.moveLob(session(), id, newPath);
        commit();
        
        // data retrieval
        getTransaction();
        byte[] output = ls.readBlob(session(), newPath, 0, data.length);
        commit();
        Assert.assertArrayEquals(data, output);
        getTransaction();
        output = ls.readBlob(session(), newPath);
        commit();
        Assert.assertArrayEquals(data, output);
        
        getTransaction();
        ls.truncateBlob(session(), newPath, 2L);
        commit();
        getTransaction();
        Assert.assertEquals(ls.sizeBlob(session(), newPath), 2L);
        commit();
        
        getTransaction();
        ls.truncateBlob(session(), newPath, 0L);
        commit();
        
        getTransaction();
        Assert.assertEquals(ls.sizeBlob(session(), newPath), 0L);
        commit();
        getTransaction();
        ls.appendBlob(session(), newPath, data);
        commit();
        getTransaction();
        output = ls.readBlob(session(), newPath);
        commit();
        Assert.assertArrayEquals(data, output);        
        
        getTransaction();
        ls.deleteLob(session(), newPath);
        commit();
        getTransaction();
        Assert.assertFalse(ls.existsLob(session(), newPath));
        commit();
    }
    
    @Before
    public void setUp(){
        // registration
        this.ls = serviceManager().getServiceByClass(LobService.class);
        Assert.assertNotNull(ls);
        getTransaction();
        idA = UUID.randomUUID();
        idB = UUID.randomUUID();
        ls.createNewLob(session(), idA);
        ls.createNewLob(session(), idB);
        ls.linkTableBlob(session(), idA, 1);
        commit();
    }
    
    @Test
    public void blobGarbageCollector() {
        // run collector
        ls.runLobGarbageCollector();
        
        // check cleaning
        getTransaction();
        Assert.assertTrue(ls.existsLob(session(), idA));
        Assert.assertFalse(ls.existsLob(session(), idB));
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
