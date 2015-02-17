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


import com.foundationdb.TransactionContext;
import com.foundationdb.qp.operator.QueryContext;

import java.util.List;
import java.util.UUID;

public interface LobService {

    public void checkAndCleanBlobs(List<String> lobIds);
    
    public void deleteLobs(UUID[] lobIds);
    public void runLobGarbageCollector();
    
    public void createNewLob(TransactionContext tcx, UUID lobId);
    public boolean existsLob(TransactionContext tcx, UUID lobId);
    public void deleteLob(TransactionContext tcx, UUID lobId);
    public void moveLob(TransactionContext tcx, UUID oldId, UUID newId);
    public void linkTableBlob(TransactionContext tcx, UUID lobId, int tableId);
    public long sizeBlob(TransactionContext tcx, UUID lobId);
    public byte[] readBlob(TransactionContext tcx, UUID lobId, long offset, int length);
    public byte[] readBlob(TransactionContext tcx, UUID lobId);
    public void writeBlob(TransactionContext tcx, UUID lobId, long offset, byte[] data);
    public void appendBlob(TransactionContext tcx, UUID lobId, byte[] data);
    public void truncateBlob(TransactionContext tcx, UUID lobId, long size);
    public void clearAllLobs(TransactionContext tcx);
    public void verifyAccessPermission(TransactionContext tcx, QueryContext context, UUID lobId);

}



