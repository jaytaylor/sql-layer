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
import com.foundationdb.blob.*;
import com.foundationdb.directory.*;
import com.foundationdb.server.service.*;
import com.foundationdb.server.store.*;
import com.foundationdb.sql.server.*;

import java.util.*;

public class LobRoutines {
    
    private LobRoutines() {
    }

    public static String createNewLob(String schemaName) {
        return createNewLob(false, Arrays.asList(schemaName));
    }

    public static String createNewLobInTable(String schemaName, String tableName, String columnName ) {
        return createNewLob(false, Arrays.asList(schemaName,tableName, columnName));
    }
    
    public static String createNewSpecificLob(String schemaName, String id) {
        return createNewLob(true, Arrays.asList(schemaName, id));
    }
    
    public static String createNewSpecificLobInTable(String schemaName, String tableName, String columnName, String id) {
        return createNewLob(true, Arrays.asList(schemaName, tableName, columnName, id));
    }
    
    private static String createNewLob(boolean specific, List<String> path) {
        // add security check on schema (securityService.isWriteAccessible() )
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        String id;
        if (specific) {
            id = UUID.fromString(path.get(path.size()-1)).toString();
        } else {
            id = java.util.UUID.randomUUID().toString();
            List<String> newPath = Arrays.asList(Arrays.copyOf((String[])path.toArray(), path.size() + 1));
            newPath.set(newPath.size() - 1, id);
            path = newPath;
        }
        ls.createLobSubspace(tcx, path).get();
        return id;
    }
    
    public static long sizeBlob(String schemaName, String blobID) {
        return sizeBlob(Arrays.asList(schemaName, blobID));
    }

    public static long sizeBlobInTable(String schemaName, String tableName, String columnName, String blobID) {
        return sizeBlob(Arrays.asList(schemaName, tableName, columnName, blobID));
    }    
    
    private static long sizeBlob(List<String> pathElements) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElements).get();
        BlobAsync blob = ls.getBlob(ds);
        
        return blob.getSize(tcx).get().longValue();
    }
    
    public static byte[] readBlob(long offset, int length, String schemaName, String blobID) {
        return readBlob(offset, length, Arrays.asList(schemaName, blobID));
    }

    public static byte[] readBlobInTable(long offset, int length, String schemaName, String tableName, String columnName, String blobID){
        return readBlob(offset, length, Arrays.asList(schemaName, tableName, columnName, blobID));
    }
    
    private static byte[] readBlob(long offset, int length, List<String> pathElements){
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElements).get();
        BlobAsync blob = ls.getBlob(ds);
        byte[] res = blob.read(tcx, offset, length).get(); 
        return res != null ? res : new byte[]{};
    }

    public static boolean writeBlob(long offset, byte[] data, String schemaName, String blobID){
        return writeBlob(offset, data, Arrays.asList(schemaName, blobID));
    }

    public static boolean writeBlobInTable(long offset, byte[] data, String schemaName, String tableName, String columnName, String blobID){
        return writeBlob(offset, data, Arrays.asList(schemaName, tableName, columnName, blobID));
    }
    
    private static boolean writeBlob(long offset, byte[] data, List<String> pathElements){
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElements).get();
        BlobAsync blob = ls.getBlob(ds);

        blob.write(tcx, offset, data).get();
        return true;
    }

    public static boolean appendBlob(byte[] data, String schemaName, String blobId) {
        return appendBlob(data, Arrays.asList(schemaName, blobId));
    }

    public static boolean appendBlobInTable(byte[] data, String schemaName, String tableName, String columnName, String blobId) {
        return appendBlob(data, Arrays.asList(schemaName, tableName, columnName, blobId));
    }
    
    private static boolean appendBlob(byte[] data, List<String> pathElements){
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElements).get();
        BlobAsync blob = ls.getBlob(ds);    

        blob.append(tcx, data).get();
        return true;
    }

    public static boolean truncateBlob(long newLength, String schemaName, String blob_id) {
        return truncateBlob(newLength, Arrays.asList(schemaName, blob_id));
    }

    public static boolean truncateBlobInTable(long newLength, String schemaName, String tableName, String columnName, String blob_id) {
        return truncateBlob(newLength, Arrays.asList(schemaName, tableName, columnName, blob_id));
    }
    
    private static boolean truncateBlob(long newLength, List<String> pathElements) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElements).get();
        BlobAsync blob = ls.getBlob(ds);

        blob.truncate(tcx, newLength).get();
        return true;
    }

    public static boolean moveBlob( String schemaName, String blobId, String newSchemaName, String tableName, String columnName) {
        return moveBlob(Arrays.asList(schemaName, blobId), Arrays.asList(newSchemaName, tableName, columnName, blobId));
    }
    
    private static boolean moveBlob( List<String> pathElementsOld, List<String> pathElementsNew){
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElementsOld).get();
        
        ls.moveLob(tcx, ds, pathElementsNew).get();
        return true;
    }

    public static boolean deleteBlob( String schemaName, String blobId){
        return deleteBlob(Arrays.asList(schemaName, blobId));
    }

    public static boolean deleteBlobInTable( String schemaName, String tableName, String columnName, String blobId){
        return deleteBlob(Arrays.asList(schemaName, tableName, columnName, blobId));
    }
    
    private static boolean deleteBlob( List<String> pathElements){
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        
        ls.removeLob(tcx, pathElements).get();
        return true;
    }

}
