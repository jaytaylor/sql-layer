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

import com.foundationdb.*;
import com.foundationdb.async.*;
import com.foundationdb.blob.BlobAsync;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.subspace.Subspace;
import com.google.inject.*;

import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.UUID;

public class LobServiceImpl implements Service, LobService {
    private final DirectorySubspace lobDirectory;
    private final String LOB_DIRECTORY = "lobs";

    @Inject
    public LobServiceImpl(FDBHolder fdbHolder) {
        this.lobDirectory = fdbHolder.getRootDirectory().create(fdbHolder.getTransactionContext(), Arrays.asList(LOB_DIRECTORY)).get();
    }
    
    @Override
    public BlobAsync getBlob(Subspace subspace) {
        // perform more test to verify it it is a blob subspace, once clob is also initiated
        return new BlobAsync(subspace);
    }

    @Override
    public Future<Void> removeLob(TransactionContext tcx, DirectorySubspace lob) { 
        return lob.remove(tcx); 
    }

    @Override
    public Future<DirectorySubspace> moveLob(TransactionContext tcx, final DirectorySubspace sourceSubspace, final List<String> targetPath) {
        List<String> newPath = new LinkedList<>(lobDirectory.getPath());
        newPath.addAll(targetPath);
        return sourceSubspace.moveTo(tcx, newPath);
    }

    @Override
    public Future<DirectorySubspace> getOrCreateLobSubspace(TransactionContext tcx, final String schemaName, final String tableName, final String columnName, final UUID id) {
        return getOrCreateLobSubspace(tcx, Arrays.asList(schemaName, tableName, columnName, id.toString()));
    }
    
    @Override
    public Future<DirectorySubspace> getOrCreateLobSubspace(TransactionContext tcx, final List<String> path) {
        return lobDirectory.createOrOpen(tcx, path);
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
