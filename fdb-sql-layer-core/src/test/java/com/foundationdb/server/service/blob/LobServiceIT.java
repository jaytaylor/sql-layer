package com.foundationdb.server.service.blob;

import com.foundationdb.*;
import com.foundationdb.blob.*;
import com.foundationdb.directory.*;
import com.foundationdb.server.store.*;
import com.foundationdb.server.test.it.ITBase;

import java.util.*;

import org.junit.*;
import org.junit.Assert;


public class LobServiceIT extends ITBase {

    
    @Test
    public void utilizeLobService() {
        LobService ls = serviceManager().getServiceByClass(LobService.class);
        Assert.assertNotNull(ls);
        FDBHolder fdbHolder = serviceManager().getServiceByClass(FDBHolder.class);
        TransactionContext tcx = fdbHolder.getTransactionContext();

        DirectorySubspace ds = ls.getOrCreateLobSubspace(fdbHolder.getTransactionContext(), "schema", "table", "column", UUID.randomUUID()).get();

        BlobAsync blob = ls.getBlob(ds);
        byte[] input = "foo".getBytes();
        blob.append(tcx, input).get();
        Assert.assertEquals(blob.getSize(tcx).get(), new Integer(input.length));
        List<String> newPath = Arrays.asList("newTestLob");

        ls.moveLob(tcx, ds, newPath).get();

        DirectorySubspace  ds3 = ls.getOrCreateLobSubspace(tcx, newPath).get();

        BlobAsync blob2 = ls.getBlob(ds3);
        byte[] output = blob2.read(tcx).get();
        Assert.assertArrayEquals(input, output);

        ls.removeLob(tcx, ds3).get();
        Assert.assertEquals(blob2.getSize(tcx).get(), new Integer(0));
        Assert.assertFalse(ds3.exists(tcx).get());
    }
}
