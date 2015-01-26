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


import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.qp.operator.QueryContext;

import java.util.*;

public class LobRoutines {
    
    private LobRoutines() {
    }

    public static String createNewLob() {
        return createNewLob(false, null);
    }
    
    public static String createNewSpecificLob(String lobId) {
        return createNewLob(true, lobId);
    }
    
    private static String createNewLob(boolean specific, String lobId) {
        // add security check on schema (securityService.isWriteAccessible() )
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        LobService ls = serviceManager.getServiceByClass(LobService.class);
        
        if (specific) {
            // also check for valid format of id 
            lobId = UUID.fromString(lobId).toString();
        } else {
            lobId = java.util.UUID.randomUUID().toString();
        }
        ls.createNewLob(lobId);
        context.getServer().addCreatedLob(lobId);
        return lobId;
    }
    
    public static long sizeBlob(String blobId) {
        return lobService().sizeBlob(blobId);
    }
    
    public static byte[] readBlob(long offset, int length, String blobId) {
        return lobService().readBlob(blobId, offset, length);
    }
    
    public static void writeBlob(long offset, byte[] data, String blobId){
        lobService().writeBlob(blobId, offset, data);
    }

    public static void appendBlob(byte[] data, String blobId) {
        lobService().appendBlob(blobId, data); 
    }

    public static void truncateBlob(long newLength, String blobId) {
        lobService().truncateBlob(blobId, newLength);
    }

    public static void deleteBlob(String blobId){
        lobService().deleteLob(blobId);
    }

    public static void runLobGarbageCollector() {
        lobService().runLobGarbageCollector();
    }
    
    private static LobService lobService() {
        QueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        return serviceManager.getServiceByClass(LobService.class);        
    }
    
    /*
    private static void checkSchemaPermission(BlobBase blob, ServerQueryContext context, ServiceManager serviceManager, TransactionContext tcx){
        Future<Integer> ftableId = blob.getLinkedTable(tcx);
        SecurityService ss = context.getServer().getSecurityService();
        Integer tableId = ftableId.get();
        if (tableId == -1){
            return;
        }
        //String schemaName = context.getAIS().getTable(tableId).getName().getSchemaName();
        String schemaName = serviceManager.getSchemaManager().getAis(context.getSession()).getTable(tableId).getName().getSchemaName();
        if ( !ss.isAccessible(context.getSession(), schemaName)) {
            throw new LobException("Cannot find lob");
        }
    }
    */
}
