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
import com.foundationdb.server.store.*;
import com.foundationdb.server.test.it.ITBase;

import java.util.*;

import org.junit.*;


public class LobServiceIT extends ITBase {

    
    @Test
    public void utilizeLobService() {
        // registration
        LobService ls = serviceManager().getServiceByClass(LobService.class);
        Assert.assertNotNull(ls);
        FDBHolder fdbHolder = serviceManager().getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();

        // blob creation
        DirectorySubspace ds = ls.getOrCreateLobSubspace(fdbHolder.getTransactionContext(), "schema", "table", "column", UUID.randomUUID()).get();

        BlobAsync blob = ls.getBlob(ds);
        byte[] input = "foo".getBytes();
        blob.append(tcx, input).get();
        Assert.assertEquals(blob.getSize(tcx).get().longValue(), new Long(input.length).longValue());
        
        // blob transfer to new address
        List<String> newPath = Arrays.asList("newTestLob");
        ls.moveLob(tcx, ds, newPath).get();
        DirectorySubspace  ds3 = ls.getOrCreateLobSubspace(tcx, newPath).get();
        BlobAsync blob2 = ls.getBlob(ds3);
        
        // data retrieval
        byte[] output = blob2.read(tcx).get();
        Assert.assertArrayEquals(input, output);
        
        // lob removal
        ls.removeLob(tcx, ds3).get();
        Assert.assertEquals(blob2.getSize(tcx).get().longValue(), 0L);
        Assert.assertFalse(ds3.exists(tcx).get());
    }
}
