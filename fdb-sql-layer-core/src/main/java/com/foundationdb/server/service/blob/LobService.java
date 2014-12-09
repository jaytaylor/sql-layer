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
import com.foundationdb.subspace.Subspace;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.async.Future;

import com.foundationdb.blob.BlobAsync;

import java.util.*;

public interface LobService {
    public BlobAsync getBlob(Subspace subspace);
    public Future<Void> removeLob(TransactionContext tcx, DirectorySubspace lob);
    public Future<DirectorySubspace> moveLob(TransactionContext tcx, DirectorySubspace sourceSubspace, List<String> targetPath);
    public Future<DirectorySubspace> getOrCreateLobSubspace(TransactionContext tcx, String schemaName, String tableName, String columnName, java.util.UUID id);
    public Future<DirectorySubspace> getOrCreateLobSubspace(TransactionContext tcx, List<String> path);
}


