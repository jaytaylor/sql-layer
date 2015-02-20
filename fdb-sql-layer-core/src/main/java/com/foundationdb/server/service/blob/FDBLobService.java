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

import com.foundationdb.async.Future;
import com.foundationdb.async.Function;
import com.foundationdb.Transaction;
import com.foundationdb.blob.SQLBlob;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.LobException;
import com.foundationdb.TransactionContext;
import com.foundationdb.directory.NoSuchDirectoryException;
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
import java.util.List;
import java.util.ArrayList;
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
    public void checkAndCleanBlobs(List<?> lobIds) {
        DirectorySubspace ds;
        SQLBlob blob;
        TransactionContext tcx = getTxc();
        List<UUID> toDo = new ArrayList<>();        
        if (lobIds.size() > 0 ) {
            for (Object lob : lobIds) {
                // List may be over complete, existence is not sure
                if (lob instanceof String) {
                    List<String> path = Arrays.asList((String)lob);
                    if (lobDirectory.exists(tcx, path).get()) {
                        ds = lobDirectory.open(tcx, path).get();
                        blob = new SQLBlob(ds);
                        if (!blob.isLinked(tcx).get()) {
                            toDo.add(UUID.fromString((String)lob));
                        }
                    }
                } else if (lob instanceof UUID) {
                    List<String> path = Arrays.asList(lob.toString());
                    if (lobDirectory.exists(tcx, path).get()) {
                        ds = lobDirectory.open(tcx, path).get();
                        blob = new SQLBlob(ds);
                        if (!blob.isLinked(tcx).get()) {
                            toDo.add((UUID) lob);
                        }
                    }                    
                }
            }
            deleteLobs(toDo.toArray(new UUID[toDo.size()]));
        }
    }

    @Override
    public void runLobGarbageCollector() {
        List<String> lobs = lobDirectory.list(getTxc()).get();
        checkAndCleanBlobs(lobs);
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
        getTxc(session).clear(getLobSubspace(lobId).range());
        getTxc(session).clear(getLobSubspace(lobId).pack());
    }

    @Override
    public void deleteLobs(UUID[] lobIds) {
        List<Future<Boolean>> done = new ArrayList<>();
        for (UUID lob : lobIds) {
            done.add(lobDirectory.removeIfExists(getTxc(), Arrays.asList(lob.toString())));
        }
        for (Future<Boolean> item : done) {
            item.get();
        }
    }

    @Override
    public void linkTableBlob(final Session session, final UUID lobId, final int tableId) {
        getTxc(session).run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                SQLBlob blob = openBlob(session, lobId);
                if (blob.isLinked(tr).get()) {
                    if (blob.getLinkedTable(tr).get() != tableId) {
                        throw new LobException("lob is already linked to a table");
                    }
                    return null;
                }
                blob.setLinkedTable(tr, tableId).get();
                return null;
            }
        });
    }

    @Override
    public long sizeBlob(final Session session, final UUID lobId) {
        return getTxc(session).run(new Function<Transaction, Long>() {
            @Override
            public Long apply(Transaction tr) {
                SQLBlob blob = openBlob(session, lobId);
                return blob.getSize(tr).get();
            }
        });
    }

    @Override
    public byte[] readBlob(final Session session, final UUID lobId, final long offset, final int length) {
        byte[] res =  getTxc(session).run(new Function<Transaction, byte[]>() {
            @Override
            public byte[] apply(Transaction tr) {
                SQLBlob blob = openBlob(session, lobId);
                return blob.read(tr, offset, length).get();
            }
        });
        return res != null ? res : new byte[]{};
    }

    @Override
    public byte[] readBlob(final Session session, final UUID lobId) {
        return getTxc(session).run(new Function<Transaction, byte[]>() {
            @Override
            public byte[] apply(Transaction tr) {
                SQLBlob blob = openBlob(session, lobId);
                return blob.read(tr).get();
            }
        });
    }

    @Override
    public void writeBlob(final Session session, final UUID lobId, final long offset, final byte[] data) {
        getTxc(session).run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                SQLBlob blob = openBlob(session, lobId);
                blob.write(tr, offset, data).get();
                return null;
            }
        });
    }

    @Override
    public void appendBlob(final Session session, final UUID lobId, final byte[] data) {
            getTxc(session).run(new Function<Transaction, Void>() {
                @Override
                public Void apply(Transaction tr) {
                    SQLBlob blob = openBlob(session, lobId);
                    blob.append(tr, data).get();
                    return null;
                }
            });
    }

    @Override
    public void truncateBlob(final Session session, final UUID lobId, final long size) {
        getTxc(session).run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                SQLBlob blob = openBlob(session, lobId);
                blob.truncate(tr, size).get();
                return null;
            }
        });
    }

    @Override
    public void clearAllLobs(Session session) { 
        TransactionContext txc = getTxc(session);
        lobDirectory.removeIfExists(txc).get();
        lobDirectory = fdbHolder.getRootDirectory().create(txc, Arrays.asList(LOB_DIRECTORY)).get();
    }
    
    @Override
    public void verifyAccessPermission(final Session session, final QueryContext context, final UUID lobId) {
        getTxc(session).run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                SQLBlob blob = openBlob(session, lobId);
                Integer tableId = blob.getLinkedTable(tr).get();
                if (tableId == null) {
                    return null;
                }
                String schemaName = context.getServiceManager().getSchemaManager().getAis(context.getSession()).getTable(tableId).getName().getSchemaName();
                if (!securityService.isAccessible(context.getSession(), schemaName)) {
                    throw new LobException("Cannot find lob");
                }
                return null;
            }
        });
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
    
    private TransactionContext getTxc(){
        return fdbHolder.getTransactionContext();
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
