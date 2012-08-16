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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class AlterTableCAOIIT extends AlterTableITBase {
    private static final String A_TABLE = "a";
    private static final TableName A_NAME = new TableName(SCHEMA, A_TABLE);
    private int cid;
    private int aid;
    private int oid;
    private int iid;

    @After @Before
    public void clearTableIDs() {
        cid = aid = oid = iid = -1;
    }

    private void createAndLoadCAOI(boolean aGrouped, boolean oGrouped, boolean iGrouped) {
        cid = createTable(C_NAME, "id int not null primary key, cc varchar(5)" );
        aid = createTable(A_NAME, "id int not null primary key, cid int, aa varchar(5)"+
                (aGrouped ? "," + akibanFK("cid", C_TABLE, "id") : ""));
        oid = createTable(O_NAME, "id int not null primary key, cid int, oo varchar(5)"+
                (oGrouped ? "," + akibanFK("cid", C_TABLE, "id") : ""));
        iid = createTable(I_NAME, "id int not null primary key, oid int, ii varchar(5)"+
                (iGrouped ? "," + akibanFK("oid", O_TABLE, "id") : ""));
        writeRows(
                createNewRow(cid, 1L, "1"),
                    createNewRow(aid, 10L, 1L, "11"),
                    createNewRow(oid, 10L, 1L, "11"),
                        createNewRow(iid, 100L, 10L, "110"),
                        createNewRow(iid, 101L, 10L, "111"),
                    createNewRow(oid, 11L, 1L, "12"),
                        createNewRow(iid, 111L, 11L, "122"),
                createNewRow(cid, 2L, "2"),                     // No children
                // No cust(3L)
                    createNewRow(oid, 30L, 3L, "33"),           // Level 1 orphan
                        createNewRow(iid, 300L, 30L, "330"),
                createNewRow(cid, 4L, "4"),
                    createNewRow(aid, 40L, 4L, "44"),
                    createNewRow(aid, 41L, 4L, "45"),
                    // No 40 order
                        createNewRow(iid, 400L, 40L, "440"),    // Level 2 orphan
                        createNewRow(iid, 401L, 40L, "441"),    // Level 2 orphan
                // No cust(5L)
                    createNewRow(aid, 50L, 5L, "55")            // Level 1 orphan
        );
    }

    private void groupsMatch(TableName name1, TableName... names) {
        UserTable t1 = getUserTable(name1);
        for(TableName name : names) {
            UserTable t2 = getUserTable(name);
            assertSame("Groups match for " + name1 + " and " + name, t1.getGroup(), t2.getGroup());
        }
    }

    private void groupsDiffer(TableName name1, TableName... names) {
        UserTable t1 = getUserTable(name1);
        for(TableName name : names) {
            UserTable t2 = getUserTable(name);
            assertNotSame("Groups differ for " + name1 + " and " + name, t1.getGroup(), t2.getGroup());
        }
    }


    //
    // SET DATA TYPE
    //

    @Test
    public void setDataType_C_cid() {
        createAndLoadCAOI(true, true, true);
        runAlter("ALTER TABLE "+C_TABLE+" ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsDiffer(C_NAME, A_NAME, O_NAME, I_NAME);
        groupsDiffer(A_NAME, O_NAME, I_NAME);
        groupsMatch(O_NAME, I_NAME);
    }

    @Test
    public void setDataType_A_aid() {
        createAndLoadCAOI(true, true, true);
        runAlter("ALTER TABLE "+A_TABLE+" ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void setDataType_O_oid() {
        createAndLoadCAOI(true, true, true);
        runAlter("ALTER TABLE "+O_TABLE+" ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsDiffer(I_NAME, C_NAME, A_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
    }

    @Test
    public void setDataType_I_oid() {
        createAndLoadCAOI(true, true, true);
        runAlter("ALTER TABLE "+I_TABLE+" ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }
}
