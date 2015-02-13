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

public interface LobService {

    public void deleteLobs(String[] lobIds);
    public void checkAndCleanBlobs(List<String> lobIds);
    public void runLobGarbageCollector(); 
    
    public void createNewLob(TransactionContext tcx, String lobId);
    public boolean existsLob(TransactionContext tcx, String lobId);
    public void deleteLob(TransactionContext tcx, String lobId);
    public void moveLob(TransactionContext tcx, String oldId, String newId);
    public void linkTableBlob(TransactionContext tcx, String lobId, int tableId);
    public long sizeBlob(TransactionContext tcx, String lobId);
    public byte[] readBlob(TransactionContext tcx, String lobId, long offset, int length);
    public byte[] readBlob(TransactionContext tcx, String lobId);
    public void writeBlob(TransactionContext tcx, String lobId, long offset, byte[] data);
    public void appendBlob(TransactionContext tcx, String lobId, byte[] data);
    public void truncateBlob(TransactionContext tcx, String lobId, long size);
    public void clearAllLobs(TransactionContext tcx);
    public void verifyAccessPermission(TransactionContext tcx, QueryContext context, String lobId);
}



