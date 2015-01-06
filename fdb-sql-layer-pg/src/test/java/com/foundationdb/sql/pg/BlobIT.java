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

package com.foundationdb.sql.pg;

import com.foundationdb.*;
import com.foundationdb.server.service.blob.LobRoutines;
import com.foundationdb.server.service.blob.LobService;

import com.foundationdb.server.store.*;
import org.junit.*;

import java.sql.*;
import java.util.*;

public class BlobIT extends PostgresServerITBase {
    
    @Test
    public void testCleanUpLobs() throws Exception {
        Connection conn = getConnection();
        String idA =  UUID.randomUUID().toString();
        PreparedStatement pstmt = conn.prepareCall("CALL sys.create_specific_blob( ? )");
        pstmt.setString(1, idA);
        pstmt.execute();
        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        Assert.assertTrue(ls.existsLob(tcx, Arrays.asList(idA)).get());
        conn.close();
        // time out is needed to have close finallize properly 
        Thread.sleep(10);
        Assert.assertFalse(ls.existsLob(tcx, Arrays.asList(idA)).get());        
    }

    private byte[] generateBytes(int length) {
        byte[] inp = new byte[length];
        Random random = new Random();
        random.nextBytes(inp);
        return inp;
    }
}
