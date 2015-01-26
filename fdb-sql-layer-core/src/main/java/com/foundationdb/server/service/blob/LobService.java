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


import java.util.List;

public interface LobService {

    public void createNewLob(String lobId);
    public boolean existsLob(String lobId);
    public void deleteLob(String lobId);
    public void deleteLobs(String[] lobIds);
    public void moveLob(String oldId, String newId);

    public void checkAndCleanBlobs(List<String> lobIds); // return lon with number of blobs deleted
    public void runLobGarbageCollector(); // return long with number of blobs deleted?
    
    public void linkTableBlob(String lobId, int tableId);
    public long sizeBlob(String lobId);
    public byte[] readBlob(String lobId, long offset, int length);
    public byte[] readBlob(String lobId);
    public void writeBlob(String lobId, long offset, byte[] data);
    public void appendBlob(String lobId, byte[] data);
    public void truncateBlob(String lobId, long size);
    // copyBlob(String lobIdA, String LobIdB)
    
}



