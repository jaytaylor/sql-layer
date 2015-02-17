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
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.LobException;
import com.foundationdb.TransactionContext;
import com.foundationdb.directory.NoSuchDirectoryException;
import com.foundationdb.server.service.*;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.store.FDBHolder;
import com.google.inject.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

public class LobServiceImpl implements Service, LobService {
    private DirectorySubspace lobDirectory;
    private FDBHolder fdbHolder;
    private final SecurityService securityService;
    private final ServiceManager serviceManager;
    private final String LOB_DIRECTORY = "lobs";
    
    @Inject
    public LobServiceImpl(ServiceManager serviceManager, SecurityService securityService) {
        this.serviceManager = serviceManager;
        this.securityService = securityService;
    }
    
    @Override
    public void checkAndCleanBlobs(List<?> lobIds) {
        DirectorySubspace ds;
        SQLBlob blob;
        TransactionContext tcx = getTcx();
        List<UUID> toDo = new ArrayList<>();        
        if (lobIds.size() > 0 ) {
            for (Object lob : lobIds) {
                if (lob instanceof String) {
                    List<String> path = Arrays.asList((String)lob);
                    // List may be over complete, existence is not sure
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
                            toDo.add((UUID)lob);
                        }
                    }                    
                }
            }
            deleteLobs(toDo.toArray(new UUID[toDo.size()]));
        }
    }

    @Override
    public void runLobGarbageCollector() {
        List<String> lobs = lobDirectory.list(getTcx()).get();
        checkAndCleanBlobs(lobs);
    }

    
    @Override
    public void createNewLob(TransactionContext tcx, UUID lobId) {
        lobDirectory.create(tcx, Arrays.asList(lobId.toString())).get();
    }

    @Override
    public boolean existsLob(TransactionContext tcx, UUID lobId) {
        return lobDirectory.exists(tcx, Arrays.asList(lobId.toString())).get();
    }

    @Override
    public void deleteLob(TransactionContext tcx, UUID lobId) {
        lobDirectory.removeIfExists(tcx, Arrays.asList(lobId.toString())).get();
    }

    @Override
    public void deleteLobs(UUID[] lobIds) {
        List<Future<Boolean>> done = new ArrayList<>();
        for (UUID lob : lobIds) {
            done.add(lobDirectory.removeIfExists(getTcx(), Arrays.asList(lob.toString())));
        }
        for (Future<Boolean> item : done) {
            item.get();
        }
    }

    @Override
    public void moveLob(TransactionContext tcx, final UUID oldId, UUID newId) {
        List<String> newPathTmp = new ArrayList<>(lobDirectory.getPath());
        newPathTmp.add(newId.toString());
        final List<String> newPath = new ArrayList<>(newPathTmp);
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                DirectorySubspace ds = lobDirectory.open(tr, Arrays.asList(oldId.toString())).get();
                ds.moveTo(tr, newPath).get();
                return null;
            }
        });
    }

    @Override
    public void linkTableBlob(TransactionContext tcx, final UUID lobId, final int tableId) {
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                SQLBlob blob = openBlob(tr, lobId);
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
    public long sizeBlob(TransactionContext tcx, final UUID lobId) {
        return tcx.run(new Function<Transaction, Long>() {
            @Override
            public Long apply(Transaction tr) {
                SQLBlob blob = openBlob(tr, lobId);
                return blob.getSize(tr).get();
            }
        });
    }

    @Override
    public byte[] readBlob(TransactionContext tcx, final UUID lobId, final long offset, final int length) {
        byte[] res =  tcx.run(new Function<Transaction, byte[]>() {
            @Override
            public byte[] apply(Transaction tr) {
                SQLBlob blob = openBlob(tr, lobId);
                return blob.read(tr, offset, length).get();
            }
        });
        return res != null ? res : new byte[]{};
    }

    @Override
    public byte[] readBlob(TransactionContext tcx, final UUID lobId) {
        return tcx.run(new Function<Transaction, byte[]>() {
            @Override
            public byte[] apply(Transaction tr) {
                SQLBlob blob = openBlob(tr, lobId);
                return blob.read(tr).get();
            }
        });
    }

    @Override
    public void writeBlob(TransactionContext tcx, final UUID lobId, final long offset, final byte[] data) {
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                SQLBlob blob = openBlob(tr, lobId);
                blob.write(tr, offset, data).get();
                return null;
            }
        });
    }

    @Override
    public void appendBlob(TransactionContext tcx, final UUID lobId, final byte[] data) {
            tcx.run(new Function<Transaction, Void>() {
                @Override
                public Void apply(Transaction tr) {
                    SQLBlob blob = openBlob(tr, lobId);
                    blob.append(tr, data).get();
                    return null;
                }
            });
    }

    @Override
    public void truncateBlob(TransactionContext tcx, final UUID lobId, final long size) {
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                SQLBlob blob = openBlob(tr, lobId);
                blob.truncate(tr, size).get();
                return null;
            }
        });
    }

    @Override
    public void clearAllLobs(TransactionContext tcx) {
        lobDirectory.removeIfExists(tcx).get();
        lobDirectory = fdbHolder.getRootDirectory().create(tcx, Arrays.asList(LOB_DIRECTORY)).get();
    }
    
    @Override
    public void verifyAccessPermission(TransactionContext tcx, final QueryContext context, final UUID lobId) {
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                SQLBlob blob = openBlob(tr, lobId);
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

    private SQLBlob openBlob(TransactionContext tcx, UUID lobId) {
        try {
            DirectorySubspace ds = lobDirectory.open(tcx, Arrays.asList(lobId.toString())).get();
            return new SQLBlob(ds);
        }
        catch (NoSuchDirectoryException nsde) {
            throw new LobException("lob with id: "+ lobId +" does not exist");
        }
    }
    
    private TransactionContext getTcx(){
        return fdbHolder.getTransactionContext();
    }
    
    @Override
    public void start() {
        this.fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
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
