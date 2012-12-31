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

package com.akiban.server.test.it.nopk;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

public final class NoPkIT extends ITBase {
    @Test
    public void replAkiban() throws Exception {
        int tableId = createTable("test", "REPL_C", "I INT");
        writeRows(
                // REPL_C is a no-PK table, so it has an Akiban-supplied __akiban_pk column, (guaranteed to be the
                // last column). Need to provide a non-null value. This value is ignored, but it must be non-null.
                createNewRow(tableId, 2L, -1L),
                createNewRow(tableId, 2L, -1L)
        );

        expectFullRows( tableId,
                // expectFullRows checks user-visible data, and so does not check the __akiban_pk column.
                createNewRow(tableId, 2L),
                createNewRow(tableId, 2L)
        );
    }

    // Inspired by bug 1023944
    @Test
    public void GIWithPKLessLeafTable_dropLeaf()
    {
        int t1 =
            createTable("schema", "t1",
                        "id int not null",
                        "primary key(id)");
        int t2 =
            createTable("schema", "t2",
                        "id int",
                        "ref int",
                        "grouping foreign key(id) references t1(id)");
        writeRows(
            createNewRow(t1, 1L),
            createNewRow(t1, 2L),
            createNewRow(t1, 3L));
        writeRows( // Bug 1023945 produces NPE when writing rows to t2
                   createNewRow(t2, 1L, 1L),
                   createNewRow(t2, 2L, 1L),
                   createNewRow(t2, 3L, 1L));
        createGroupIndex("t1", "gi", "t2.ref, t2.id, t1.id", Index.JoinType.LEFT);
        ddl().dropTable(session(), new TableName("schema", "t2"));
    }

    // Inspired by bug 1023945
    @Test
    public void GIWithPKLessLeafTable_populate()
    {
        int t1 =
            createTable("schema", "t1",
                        "id int not null",
                        "primary key(id)");
        int t2 =
            createTable("schema", "t2",
                        "id int",
                        "ref int",
                        "grouping foreign key(id) references t1(id)");
        createGroupIndex("t1", "gi", "t2.ref, t2.id, t1.id", Index.JoinType.LEFT);
        writeRows(
            createNewRow(t1, 1L),
            createNewRow(t1, 2L),
            createNewRow(t1, 3L));
        writeRows( // Bug 1023945 produces NPE when writing rows to t2
            createNewRow(t2, 1L, 1L),
            createNewRow(t2, 2L, 1L),
            createNewRow(t2, 3L, 1L));
    }
}