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
import com.foundationdb.blob.BlobBase;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.server.error.LobException;
import com.foundationdb.TransactionContext;
import com.foundationdb.directory.NoSuchDirectoryException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.store.FDBHolder;
import com.google.inject.*;

import java.util.*;

public class LobServiceImpl implements Service, LobService {
    private final DirectorySubspace lobDirectory;
    private final FDBHolder fdbHolder;
    // update with configurable value
    private final String LOB_DIRECTORY = "lobs";
    
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
        List<Future<Boolean>> done = new ArrayList<Future<Boolean>>();
        for (int i = 0; i < lobIds.length; i++) {
            done.add(lobDirectory.removeIfExists(getTcx(), Arrays.asList(lobIds[i])));
        }
        for (Future<Boolean> item : done) {
            item.get();
        }
    }

    @Override
    public void moveLob(String oldId, String newId) {
        List<String> newPath = Arrays.asList(newId);
        DirectorySubspace ds = lobDirectory.open(getTcx(), Arrays.asList(oldId)).get();
        ds.moveTo(getTcx(), newPath).get();
    }

    @Override
    public void checkAndCleanBlobs(List<String> lobIds) {
        DirectorySubspace ds;
        BlobBase blob;
        TransactionContext tcx = getTcx();
        List<String> toDo = new ArrayList<String>();        
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
    public void linkTableBlob(String lobId, int tableId) {
        BlobBase blob = openBlob(lobId);
        if (blob.isLinked(getTcx()).get()) {
            if (blob.getLinkedTable(getTcx()).get() != tableId) {
                throw new LobException("lob is already linked to table");
            }
        }
        blob.setLinkedTable(getTcx(), tableId).get();
    }

    @Override
    public long sizeBlob(String lobId) {
        BlobBase blob = openBlob(lobId);
        return blob.getSize(getTcx()).get().longValue();
    }

    @Override
    public byte[] readBlob(String lobId, long offset, int length) {
        BlobBase blob = openBlob(lobId);
        byte[] res = blob.read(getTcx(), offset, length).get();
        return res != null ? res : new byte[]{};
    }
    
    @Override
    public byte[] readBlob(String lobId) {
        return openBlob(lobId).read(getTcx()).get();
    }

    @Override
    public void writeBlob(String lobId, long offset, byte[] data) {
        BlobBase blob = openBlob(lobId);
        blob.write(getTcx(), offset, data).get();
    }

    @Override
    public void appendBlob(String lobId, byte[] data) {
        BlobBase blob = openBlob(lobId);
        blob.append(getTcx(), data).get();
    }

    @Override
    public void truncateBlob(String lobId, long size) {
        BlobBase blob = openBlob(lobId);
        blob.truncate(getTcx(), size).get();
    }

    private BlobBase openBlob(String lobId) {
        try {
            DirectorySubspace ds = lobDirectory.open(getTcx(), Arrays.asList(lobId)).get();
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
