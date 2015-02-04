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

import com.foundationdb.async.Future;
import com.foundationdb.async.Function;
import com.foundationdb.Transaction;
import com.foundationdb.blob.BlobBase;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.server.error.LobException;
import com.foundationdb.TransactionContext;
import com.foundationdb.directory.NoSuchDirectoryException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.store.FDBHolder;
import com.google.inject.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class LobServiceImpl implements Service, LobService {
    private DirectorySubspace lobDirectory;
    private final FDBHolder fdbHolder;
    // update with configurable value
    private final String LOB_DIRECTORY = "lobs";
    private static final int dataChunkSize = 9500000;
    
    @Inject
    public LobServiceImpl(FDBHolder fdbHolder) {
        this.fdbHolder = fdbHolder;
        this.lobDirectory = fdbHolder.getRootDirectory().createOrOpen(fdbHolder.getTransactionContext(), Arrays.asList(LOB_DIRECTORY)).get();
    }
    
    @Override
    public void createNewLob(String lobId) {
        lobDirectory.create(getTcx(), Arrays.asList(lobId)).get();
    }

    @Override
    public boolean existsLob(String lobId) {
        return lobDirectory.exists(getTcx(), Arrays.asList(lobId)).get();
    }

    @Override
    public void deleteLob(String lobId) {
        lobDirectory.removeIfExists(getTcx(), Arrays.asList(lobId)).get();
    }

    @Override
    public void deleteLobs(String[] lobIds) {
        List<Future<Boolean>> done = new ArrayList<>();
        for (int i = 0; i < lobIds.length; i++) {
            done.add(lobDirectory.removeIfExists(getTcx(), Arrays.asList(lobIds[i])));
        }
        for (Future<Boolean> item : done) {
            item.get();
        }
    }

    @Override
    public void moveLob(final String oldId, String newId) {
        List<String> newPathTmp = new ArrayList<>(lobDirectory.getPath());
        newPathTmp.add(newId);
        final List<String> newPath = new ArrayList<>(newPathTmp);
        TransactionContext tcx = getTcx();
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                DirectorySubspace ds = lobDirectory.open(tr, Arrays.asList(oldId)).get();
                ds.moveTo(tr, newPath).get();
                return null;
            }
        });
    }

    @Override
    public void checkAndCleanBlobs(List<String> lobIds) {
        DirectorySubspace ds;
        BlobBase blob;
        TransactionContext tcx = getTcx();
        List<String> toDo = new ArrayList<>();        
        if (lobIds.size() > 0 ) {
            for (int i = 0; i < lobIds.size(); i++) {
                String lob = lobIds.get(i);
                List<String> path = Arrays.asList(lob);
                // List may be over complete, existence is not sure
                if (lobDirectory.exists(tcx, path).get()) {
                    ds = lobDirectory.open(tcx, path).get();
                    blob = new BlobBase(ds);
                    if (!blob.isLinked(tcx).get()) {
                        toDo.add(lob);
                    }
                }
            }
            deleteLobs(toDo.toArray(new String[toDo.size()]));
        }
    }

    @Override
    public void runLobGarbageCollector() {
        List<String> lobs = lobDirectory.list(getTcx()).get();
        checkAndCleanBlobs(lobs);
    }

    @Override
    public void linkTableBlob(final String lobId, final int tableId) {
        TransactionContext tcx = getTcx();
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                BlobBase blob = openBlob(tr, lobId);
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
    public long sizeBlob(final String lobId) {
        TransactionContext tcx = getTcx();
        return tcx.run(new Function<Transaction, Long>() {
            @Override
            public Long apply(Transaction tr) {
                BlobBase blob = openBlob(tr, lobId);
                return blob.getSize(tr).get();
            }
        });
    }

    @Override
    public byte[] readBlob(final String lobId, final long offset, final int length) {
        TransactionContext tcx = getTcx();
        byte[] res =  tcx.run(new Function<Transaction, byte[]>() {
            @Override
            public byte[] apply(Transaction tr) {
                BlobBase blob = openBlob(tr, lobId);
                return blob.read(tr, offset, length).get();
            }
        });
        return res != null ? res : new byte[]{};
    }
    
    @Override
    public byte[] readBlob(final String lobId) {
        TransactionContext tcx = getTcx();
        return tcx.run(new Function<Transaction, byte[]>() {
            @Override
            public byte[] apply(Transaction tr) {
                BlobBase blob = openBlob(tr, lobId);
                return blob.read(tr).get();
            }
        });
    }

    @Override
    public void writeBlob(String lobId, long offset, byte[] data) {
        BlobBase blob = openBlob(getTcx(), lobId);
        if (data.length < dataChunkSize) {
            blob.write(getTcx(), offset, data).get();
        }
        else {
            int noChunks = 1 + data.length / dataChunkSize;
            int chunk = 0;
            List<Future<Void>> done = new ArrayList<>();
            int start, end;
            while (chunk < noChunks) {
                start = chunk * dataChunkSize;
                end = (chunk + 1) * dataChunkSize;
                end = data.length < end ? data.length : end;
                done.add(blob.write(getTcx(), offset + start, Arrays.copyOfRange(data, start, end)));
                chunk++;
            }
            for (Future<Void> res : done) {
                res.get();
            }
        }
    }

    @Override
    public void appendBlob(final String lobId, final byte[] data) {
        if (data.length < dataChunkSize) {
            TransactionContext tcx = getTcx();
            tcx.run(new Function<Transaction, Void>() {
                @Override
                public Void apply(Transaction tr) {
                    BlobBase blob = openBlob(tr, lobId);
                    blob.append(tr, data).get();
                    return null;
                }
            });
        }
        else {
            BlobBase blob = openBlob(getTcx(), lobId);
            int noChunks = 1 + data.length / dataChunkSize;
            int chunk = 0;
            int start, end;
            while (chunk < noChunks) {
                start = chunk * dataChunkSize;
                end = (chunk + 1) * dataChunkSize;
                end = data.length < end ? data.length : end;
                blob.append(getTcx(), Arrays.copyOfRange(data, start, end)).get();
                chunk++;
            }
        }
    }

    @Override
    public void truncateBlob(final String lobId, final long size) {
        TransactionContext tcx = getTcx();
        tcx.run(new Function<Transaction, Void>() {
            @Override
            public Void apply(Transaction tr) {
                BlobBase blob = openBlob(tr, lobId);
                blob.truncate(tr, size).get();
                return null;
            }
        });
    }

    @Override
    public void clearAllLobs() {
        lobDirectory.removeIfExists(getTcx()).get();
        lobDirectory = fdbHolder.getRootDirectory().create(fdbHolder.getTransactionContext(), Arrays.asList(LOB_DIRECTORY)).get();
    }
    
    private BlobBase openBlob(TransactionContext tcx, String lobId) {
        try {
            DirectorySubspace ds = lobDirectory.open(tcx, Arrays.asList(lobId)).get();
            return new BlobBase(ds);
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
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
        stop();
    }
}
