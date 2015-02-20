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
import com.foundationdb.blob.SQLBlob;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.LobException;
import com.foundationdb.TransactionContext;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.tuple.Tuple;
import com.google.inject.Inject;

import java.util.Arrays;
import java.util.UUID;

public class FDBLobService implements Service, LobService {
    private static byte[] LOB_EXISTS = {0x01};
    private DirectorySubspace lobDirectory;
    private FDBHolder fdbHolder;
    private FDBTransactionService transactionService;
    private final SecurityService securityService;
    private final ServiceManager serviceManager;
    private final String LOB_DIRECTORY = "lobs";
    
    @Inject
    public FDBLobService(ServiceManager serviceManager, SecurityService securityService) {
        this.serviceManager = serviceManager;
        this.securityService = securityService;
    }

    @Override
    public void createNewLob(Session session, final UUID lobId) {
        if (existsLob(session, lobId)) {
            throw new LobException("Lob already exists");
        }
        getTxc(session).set(getLobSubspace(lobId).pack(), LOB_EXISTS);
        transactionService.addCallback(session, TransactionService.CallbackType.PRE_COMMIT, new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                try {
                    SQLBlob blob = openBlob(session, lobId);
                    if (!(blob.isLinked(getTxc(session))).get()) {
                        deleteLob(session, lobId);
                    }
                } catch (LobException le){
                    // lob already gone
                }
            }
        });
    }

    @Override
    public boolean existsLob(Session session, UUID lobId) {
        return getTxc(session).get(getLobSubspace(lobId).pack()).get() != null;
    }

    @Override
    public void deleteLob(Session session, UUID lobId) {
        // clear content
        getTxc(session).clear(getLobSubspace(lobId).range());
        // clear existence value
        getTxc(session).clear(getLobSubspace(lobId).pack());
    }

    @Override
    public void linkTableBlob(final Session session, final UUID lobId, final int tableId) {
        Transaction tr = getTxc(session);
        SQLBlob blob = openBlob(session, lobId);
        if (blob.isLinked(tr).get()) {
            if (blob.getLinkedTable(tr).get() != tableId) {
                throw new LobException("lob is already linked to a table");
            }
            return;
        }
        blob.setLinkedTable(tr, tableId).get();
    }

    @Override
    public long sizeBlob(final Session session, final UUID lobId) {
        SQLBlob blob = openBlob(session, lobId);
        return blob.getSize(getTxc(session)).get();
    }

    @Override
    public byte[] readBlob(final Session session, final UUID lobId, final long offset, final int length) {
        SQLBlob blob = openBlob(session, lobId);
        byte[] res = blob.read(getTxc(session), offset, length).get();
        return res != null ? res : new byte[]{};
    }

    @Override
    public byte[] readBlob(final Session session, final UUID lobId) {
        SQLBlob blob = openBlob(session, lobId);
        return blob.read(getTxc(session)).get();
    }

    @Override
    public void writeBlob(final Session session, final UUID lobId, final long offset, final byte[] data) {
        SQLBlob blob = openBlob(session, lobId);
        blob.write(getTxc(session), offset, data).get();
    }

    @Override
    public void appendBlob(final Session session, final UUID lobId, final byte[] data) {
        SQLBlob blob = openBlob(session, lobId);
        blob.append(getTxc(session), data).get();
    }

    @Override
    public void truncateBlob(final Session session, final UUID lobId, final long size) {
        SQLBlob blob = openBlob(session, lobId);
        blob.truncate(getTxc(session), size).get();
    }

    @Override
    public void clearAllLobs(Session session) { 
        TransactionContext txc = getTxc(session);
        lobDirectory.removeIfExists(txc).get();
        lobDirectory = fdbHolder.getRootDirectory().create(txc, Arrays.asList(LOB_DIRECTORY)).get();
    }
    
    @Override
    public void verifyAccessPermission(final Session session, final QueryContext context, final UUID lobId) {
        SQLBlob blob = openBlob(session, lobId);
        Integer tableId = blob.getLinkedTable(getTxc(session)).get();
        if (tableId == null) {
            return;
        }
        String schemaName = context.getServiceManager().getSchemaManager().getAis(context.getSession()).getTable(tableId).getName().getSchemaName();
        if (!securityService.isAccessible(context.getSession(), schemaName)) {
            throw new LobException("Cannot find lob");
        }
    }

    private SQLBlob openBlob(Session session, UUID lobId) {
        if(existsLob(session, lobId)) {
            return new SQLBlob(getLobSubspace(lobId));
        }
        else {
            throw new LobException("lob with id: "+ lobId +" does not exist");
        }
    }
    
    private Subspace getLobSubspace(UUID lobId) {
        return lobDirectory.get(Tuple.from(AkGUID.uuidToBytes(lobId)));
    }
    
    private Transaction getTxc(Session session) {
        return transactionService.getTransaction(session).getTransaction();
    } 
    
    @Override
    public void start() {
        this.fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionService ts = serviceManager.getServiceByClass(TransactionService.class);
        if (ts instanceof FDBTransactionService) {
            this.transactionService = (FDBTransactionService)ts;
        }
        this.lobDirectory = fdbHolder.getRootDirectory().createOrOpen(fdbHolder.getTransactionContext(), Arrays.asList(LOB_DIRECTORY)).get();
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
        stop();
    }
}
