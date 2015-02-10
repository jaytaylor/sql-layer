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


import com.foundationdb.TransactionContext;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.server.error.LobException;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.qp.operator.QueryContext;

import java.util.*;

public class LobRoutines {
    
    private LobRoutines() {
    }

    public static String createNewLob() {
        return createNewLob(false, null);
    }
    
    public static String createNewSpecificLob(String lobId) {
        return createNewLob(true, lobId);
    }
    
    private static String createNewLob(boolean specific, String lobId) {
        // add security check on schema (securityService.isWriteAccessible() )
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        final LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        
        if (specific) {
            // also check for valid format of id 
            lobId = UUID.fromString(lobId).toString();
        } else {
            lobId = java.util.UUID.randomUUID().toString();
        }
        final String id = lobId;
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                ls.createNewLob(tr, id);
                return null;
            }
        });

        context.getServer().addCreatedLob(lobId);
        return lobId;
    }
    
    public static long sizeBlob(String blobId) {
        LobService ls = getLobService();
        TransactionContext tcx = getTransactionContext();
        return ls.sizeBlob(tcx, blobId);
    }
    
    public static void readBlob(long offset, int length, String blobId, byte[][] out) {
        out[0] = getLobService().readBlob(getTransactionContext(), blobId, offset, length);
    }
    
    public static void writeBlob(long offset, byte[] data, String blobId){
        getLobService().writeBlob(getTransactionContext(), blobId, offset, data);
    }

    public static void appendBlob(byte[] data, String blobId) {
        getLobService().appendBlob(getTransactionContext(), blobId, data); 
    }

    public static void truncateBlob(long newLength, String blobId) {
        getLobService().truncateBlob(getTransactionContext(), blobId, newLength);
    }

    public static void deleteBlob(String blobId){
        getLobService().deleteLob(getTransactionContext(),blobId);
    }

    public static void runLobGarbageCollector() {
        getLobService().runLobGarbageCollector();
    }
    
    private static LobService getLobService() {
        QueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        return serviceManager.getServiceByClass(LobService.class);        
    }

    private static TransactionContext getTransactionContext() {
        QueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        TransactionService  txnService= serviceManager.getServiceByClass(TransactionService.class);
        if (txnService instanceof  FDBTransactionService ) {
            if (txnService.isTransactionActive(context.getSession())) {
                return ((FDBTransactionService) txnService).getTransaction(context.getSession()).getTransaction();
            }
            else {
                // or open and close a specific transaction?
                FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
                return fdbHolder.getTransactionContext();
            }
        }
        else
            throw new LobException("Unsupported lobs");
        
    }

    /*
    private static void checkSchemaPermission(BlobBase blob, ServerQueryContext context, ServiceManager serviceManager, TransactionContext tcx){
        Future<Integer> ftableId = blob.getLinkedTable(tcx);
        SecurityService ss = context.getServer().getSecurityService();
        Integer tableId = ftableId.get();
        if (tableId == -1){
            return;
        }
        //String schemaName = context.getAIS().getTable(tableId).getName().getSchemaName();
        String schemaName = serviceManager.getSchemaManager().getAis(context.getSession()).getTable(tableId).getName().getSchemaName();
        if ( !ss.isAccessible(context.getSession(), schemaName)) {
            throw new LobException("Cannot find lob");
        }
    }
    */
}
