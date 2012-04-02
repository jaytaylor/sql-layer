/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.bugs.bug696156;

import com.akiban.ais.model.AISBuilder;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MultipleNullUniqueIndexIT  extends ITBase {

    @Test
    public void reportTestCase() throws InvalidOperationException {
        String SCHEMA = "test";
        String TABLE = "t1";
        String COLUMN = "c1";
        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, COLUMN, 0, "TINYINT", null, null, true, true, null, null);
        builder.index(SCHEMA, TABLE, "c1", true, "UNIQUE");
        builder.indexColumn(SCHEMA, TABLE, COLUMN, COLUMN, 0, true, null);
        ddl().createTable(session(), builder.akibanInformationSchema().getUserTable(SCHEMA, TABLE));
        updateAISGeneration();
        final int tid = tableId(SCHEMA, TABLE);
        
        writeRows(createNewRow(tid, null, -1L));
        writeRows(createNewRow(tid, null, -1L));
        expectFullRows(tid,
                       createNewRow(tid, (Object)null),
                       createNewRow(tid, (Object)null));
    }

    @Test
    public void singleColumnUniqueWithNulls() throws InvalidOperationException {
        final int tid = createTable("test", "t1", "id int not null primary key, name varchar(32), unique(name)");
        writeRows(createNewRow(tid, 1, "abc"),
                  createNewRow(tid, 2, "def"),
                  createNewRow(tid, 3, null),
                  createNewRow(tid, 4, "ghi"),
                  createNewRow(tid, 5, null));
        assertEquals(5, scanAll(new ScanAllRequest(tid, null)).size());

        try {
            writeRows(createNewRow(tid, 6, "abc"));
            Assert.fail("DuplicateKeyException expected");
        }
        catch(DuplicateKeyException e) {
        }
    }

    @Test
    public void multiColumnUniqueWithNulls() throws InvalidOperationException {
        final int tid = createTable("test", "t1", "id int not null primary key, seg1 int, seg2 int, seg3 int, unique(seg1,seg2,seg3)");
        writeRows(createNewRow(tid, 1, 1, 1, 1),
                  createNewRow(tid, 2, 1, 1, null),
                  createNewRow(tid, 3, 1, null, 1),
                  createNewRow(tid, 4, 1, null, null),
                  createNewRow(tid, 5, null, 1, 1),
                  createNewRow(tid, 6, null, 1, null),
                  createNewRow(tid, 7, null, null, null),
                  createNewRow(tid, 8, null, null, null));
        assertEquals(8, scanAll(new ScanAllRequest(tid, null)).size());

        try {
            writeRows(createNewRow(tid, 9, 1, 1, 1));
            Assert.fail("DuplicateKeyException expected");
        }
        catch(DuplicateKeyException e) {
        }
    }

    @Test
    public void singleColumnIndexWithNulls() throws InvalidOperationException {
        final int tid = createTable("test", "t1", "id int not null primary key, name varchar(32)");
        createIndex("test", "t1", "name", "name");
        writeRows(createNewRow(tid, 1, "abc"),
                  createNewRow(tid, 2, "def"),
                  createNewRow(tid, 3, "abc"),
                  createNewRow(tid, 4, null),
                  createNewRow(tid, 5, null));
        assertEquals(5, scanAll(new ScanAllRequest(tid, null)).size());
    }

    @Test
    public void multiColumnIndexWithNulls() throws InvalidOperationException {
        final int tid = createTable("test", "t1", "id int not null primary key, seg1 int, seg2 int");
        createIndex("test", "t1", "seg1", "seg1", "seg2");
        writeRows(createNewRow(tid, 1, 1, 1),
                  createNewRow(tid, 2, 2, 2),
                  createNewRow(tid, 3, 1, 1),
                  createNewRow(tid, 4, 1, null),
                  createNewRow(tid, 5, null, 1),
                  createNewRow(tid, 6, 1, null),
                  createNewRow(tid, 7, null, 1),
                  createNewRow(tid, 8, null, null),
                  createNewRow(tid, 9, null, null));
        assertEquals(9, scanAll(new ScanAllRequest(tid, null)).size());
    }
}
