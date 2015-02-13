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


import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.server.error.LobException;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerCallContextStack;
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
        FDBTransactionService txnService = getTransactionService();
        final ServerQueryContext context = ServerCallContextStack.getCallingContext();
        Session session = context.getSession();
        boolean startedTransaction = false;

        if (specific) {
            // This is a check for valid format of id 
            lobId = UUID.fromString(lobId).toString();
        } else {
            lobId = java.util.UUID.randomUUID().toString();
        }
        final String id = lobId;

        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        FDBTransactionService.TransactionState txnState = txnService.getTransaction(session);
        try {
            txnState.getTransaction().run(new Function<Transaction, Void>() {
                @Override
                public Void apply(Transaction tr) {
                    LobService ls = getLobService();
                    ls.createNewLob(tr, id);
                    return null;
                }
            });
            if (startedTransaction) {
                txnService.commitOrRetryTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
        context.getServer().addCreatedLob(lobId);
        return id;
    }
    
    
    public static long sizeBlob(final String blobId) {
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        Session session = context.getSession();
        boolean startedTransaction = false;
        long size; 
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        FDBTransactionService.TransactionState txnState = txnService.getTransaction(session);
        try {
            size = txnState.getTransaction().run(new Function<Transaction, Long>() {
                @Override
                public Long apply(Transaction tr) {
                    LobService ls = getLobService();
                    ls.verifyAccessPermission(tr, context, blobId);
                    return ls.sizeBlob(tr, blobId);
                }
            });
            if (startedTransaction) {
                txnService.commitOrRetryTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
        return size;
    }
    
    public static void readBlob(final long offset, final int length, final String blobId, byte[][] out) {
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        Session session = context.getSession();
        boolean startedTransaction = false;
        byte[] output;
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        FDBTransactionService.TransactionState txnState = txnService.getTransaction(session);
        try {
            output = txnState.getTransaction().run(new Function<Transaction, byte[]>() {
                @Override
                public byte[] apply(Transaction tr) {
                    LobService ls = getLobService();
                    ls.verifyAccessPermission(tr, context, blobId);
                    return ls.readBlob(tr, blobId, offset, length);
                }
            });
            if (startedTransaction) {
                txnService.commitOrRetryTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
        out[0] = output; 
    }
    
    public static void writeBlob(final long offset, final byte[] data, final String blobId){
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        Session session = context.getSession();
        boolean startedTransaction = false;
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        FDBTransactionService.TransactionState txnState = txnService.getTransaction(session);
        try {
            txnState.getTransaction().run(new Function<Transaction, Void>() {
                @Override
                public Void apply(Transaction tr) {
                    LobService ls = getLobService();
                    ls.verifyAccessPermission(tr, context, blobId);
                    ls.writeBlob(tr, blobId, offset, data);
                    return null;
                }
            });
            if (startedTransaction) {
                txnService.commitOrRetryTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
    }

    public static void appendBlob(final byte[] data, final String blobId) {
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        Session session = context.getSession();
        boolean startedTransaction = false;
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        FDBTransactionService.TransactionState txnState = txnService.getTransaction(session);
        try {
            txnState.getTransaction().run(new Function<Transaction, Void>() {
                @Override
                public Void apply(Transaction tr) {
                    LobService ls = getLobService();
                    ls.verifyAccessPermission(tr, context, blobId);
                    ls.appendBlob(tr, blobId, data);
                    return null;
                }
            });
            if (startedTransaction) {
                txnService.commitOrRetryTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
    }

    public static void truncateBlob(final long newLength, final String blobId) {
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        Session session = context.getSession();
        boolean startedTransaction = false;
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        FDBTransactionService.TransactionState txnState = txnService.getTransaction(session);
        try {
            txnState.getTransaction().run(new Function<Transaction, Void>() {
                @Override
                public Void apply(Transaction tr) {
                    LobService ls = getLobService();
                    ls.verifyAccessPermission(tr, context, blobId);
                    ls.truncateBlob(tr, blobId, newLength);
                    return null;
                }
            });
            if (startedTransaction) {
                txnService.commitOrRetryTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
    }
    

    public static void runLobGarbageCollector() {
        getLobService().runLobGarbageCollector();
    }
    
    private static LobService getLobService() {
        QueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        return serviceManager.getServiceByClass(LobService.class);        
    }

    private static FDBTransactionService getTransactionService() {
        QueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        TransactionService txnService = serviceManager.getServiceByClass(TransactionService.class);
        if (txnService instanceof  FDBTransactionService ) {
            return ((FDBTransactionService) txnService);
        } 
        else 
            throw new LobException("Unsupported transaction service");
    }
}
