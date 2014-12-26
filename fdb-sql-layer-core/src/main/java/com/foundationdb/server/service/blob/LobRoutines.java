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

    public static String createNewLob() {
        return createNewLob(false, null);
    }
    
    public static String createNewSpecificLob(String id) {
        return createNewLob(true, id);
    }
    
    private static String createNewLob(boolean specific, String id) {
        // add security check on schema (securityService.isWriteAccessible() )
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        if (specific) {
            // also check for valid format of id 
            id = UUID.fromString(id).toString();
        } else {
            id = java.util.UUID.randomUUID().toString();
        }
        List<String> path = Arrays.asList(id);
        ls.createLobSubspace(tcx, path).get();
        return id;
    }
    
    public static long sizeBlob(String blobID) {
        return sizeBlob(Arrays.asList(blobID));
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
    
    public static byte[] readBlob(long offset, int length, String blobID) {
        return readBlob(offset, length, Arrays.asList(blobID));
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

    public static void writeBlob(long offset, byte[] data, String blobID){
        writeBlob(offset, data, Arrays.asList(blobID));
    }
   
    private static void writeBlob(long offset, byte[] data, List<String> pathElements){
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElements).get();
        BlobAsync blob = ls.getBlob(ds);

        blob.write(tcx, offset, data).get();
    }

    public static void appendBlob(byte[] data, String blobId) {
        appendBlob(data, Arrays.asList(blobId));
    }
    
    private static void appendBlob(byte[] data, List<String> pathElements){
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElements).get();
        BlobAsync blob = ls.getBlob(ds);    

        blob.append(tcx, data).get();
    }

    public static void truncateBlob(long newLength, String blob_id) {
        truncateBlob(newLength, Arrays.asList(blob_id));
    }
    
    private static void truncateBlob(long newLength, List<String> pathElements) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        DirectorySubspace ds = ls.getLobSubspace(tcx, pathElements).get();
        BlobAsync blob = ls.getBlob(ds);

        blob.truncate(tcx, newLength).get();
    }

    public static void deleteBlob(String blobId){
        deleteBlob(Arrays.asList(blobId));
    }
    
    private static void deleteBlob( List<String> pathElements){
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServer().getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        FDBHolder fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();
        
        ls.removeLob(tcx, pathElements).get();
    }

}
