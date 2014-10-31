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

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

import com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * Group altering actions on CAOI schema with conflated pk/fk all the way down.
 * Highly unlikely, but good test none-the-less.
 */
public class AlterTableCAOIConflatedKeysIT extends AlterTableITBase {
    private static final String A_TABLE = "a";
    private static final TableName A_NAME = new TableName(SCHEMA, A_TABLE);
    private int cid;
    private int aid;
    private int oid;
    private int iid;

    @After
    public void clearTableIDs() {
        cid = aid = oid = iid = -1;
    }

    @Override
    protected ChangeLevel getDefaultChangeLevel() {
        return ChangeLevel.GROUP;
    }

    @Override
    protected void createAndLoadCAOI_PK_FK(boolean cPK, boolean aPK, boolean aFK, boolean oPK, boolean oFK, boolean iPK, boolean iFK) {
        cid = createTable(C_NAME, "id int not null "+(cPK ? "primary key" : "")+", cc varchar(5)" );
        aid = createTable(A_NAME, "id int not null "+(aPK ? "primary key" : "")+", aa varchar(5)"+
                (aFK ? "," + akibanFK("id", C_TABLE, "id") : ""));
        oid = createTable(O_NAME, "id int not null "+(oPK ? "primary key" : "")+", oo varchar(5)"+
                (oFK ? "," + akibanFK("id", C_TABLE, "id") : ""));
        iid = createTable(I_NAME, "id int not null "+(iPK ? "primary key" : "")+", ii varchar(5)"+
                (iFK ? "," + akibanFK("id", O_TABLE, "id") : ""));
        // Index on non-pk, non-fk column
        createIndex(SCHEMA, C_TABLE, "cc", "cc");
        createIndex(SCHEMA, A_TABLE, "aa", "aa");
        createIndex(SCHEMA, O_TABLE, "oo", "oo");
        createIndex(SCHEMA, I_TABLE, "ii", "ii");
        // Default post indexes presence checking
        checkedIndexes.put(cid, Arrays.asList("PRIMARY", "cc"));
        checkedIndexes.put(aid, Arrays.asList("PRIMARY", "aa"));
        checkedIndexes.put(oid, Arrays.asList("PRIMARY", "oo"));
        checkedIndexes.put(iid, Arrays.asList("PRIMARY", "ii"));
        txnService().beginTransaction(session());
        // Data
        writeRows(
                row(cid, 1L, "1"),
                    row(aid, 1L, "11"),
                        row(iid, 10L, "110"),
                row(cid, 2L, "2"),                     // No children
                // No cust(3L)
                    row(oid, 3L, "33"),           // Level 1 orphan
                        row(iid, 3L, "330"),
                row(cid, 4L, "4"),
                    row(aid, 4L, "44"),
                    // No 40 order
                        row(iid, 40L, "440"),    // Level 2 orphan
                // No cust(5L)
                    row(aid, 5L, "55")            // Level 1 orphan
        );
        txnService().commitTransaction(session());
    }

    private void groupsMatch(TableName name1, TableName... names) {
        Table t1 = getTable(name1);
        for(TableName name : names) {
            Table t2 = getTable(name);
            assertSame("Groups match for " + name1 + " and " + name, t1.getGroup(), t2.getGroup());
        }
    }

    private void groupsDiffer(TableName name1, TableName... names) {
        Table t1 = getTable(name1);
        for(TableName name : names) {
            Table t2 = getTable(name);
            assertNotSame("Groups differ for " + name1 + " and " + name, t1.getGroup(), t2.getGroup());
        }
    }


    //
    // SET DATA TYPE, parent/child column
    //

    @Test
    public void setDataType_C_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + C_TABLE + " ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsDiffer(C_NAME, A_NAME, O_NAME, I_NAME);
        groupsDiffer(A_NAME, O_NAME, I_NAME);
        groupsMatch(O_NAME, I_NAME);
    }

    @Test
    public void setDataType_A_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + A_TABLE + " ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsDiffer(C_NAME, A_NAME);
        groupsMatch(C_NAME, O_NAME, I_NAME);
    }

    @Test
    public void setDataType_O_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + O_TABLE + " ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsDiffer(C_NAME, O_NAME);
        groupsDiffer(O_NAME, I_NAME);
        groupsMatch(C_NAME, A_NAME);
    }

    @Test
    public void setDataType_I_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + I_TABLE + " ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsDiffer(O_NAME, I_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
    }

    //
    // DROP COLUMN <pk and fk column>
    //

    @Test
    public void dropColumn_C_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+C_TABLE+" DROP COLUMN id");
        groupsDiffer(C_NAME, A_NAME, O_NAME, I_NAME);
        groupsDiffer(A_NAME, O_NAME, I_NAME);
        groupsMatch(O_NAME, I_NAME);
        checkIndexesInstead(C_NAME, "cc");
    }

    @Test
    public void dropColumn_A_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+A_TABLE+" DROP COLUMN id");
        groupsDiffer(C_NAME, A_NAME);
        groupsMatch(C_NAME, O_NAME, I_NAME);
        checkIndexesInstead(A_NAME, "aa");
    }

    @Test
    public void dropColumn_O_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+O_TABLE+" DROP COLUMN id");
        groupsDiffer(C_NAME, O_NAME);
        groupsDiffer(O_NAME, I_NAME);
        groupsMatch(C_NAME, A_NAME);
        checkIndexesInstead(O_NAME, "oo");
    }

    @Test
    public void dropColumn_I_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+I_TABLE+" DROP COLUMN id");
        groupsDiffer(O_NAME, I_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
        checkIndexesInstead(I_NAME, "ii");
    }

    //
    // ADD PRIMARY KEY
    //

    @Test
    public void addPrimaryKey_I() {
        createAndLoadCAOI_PK(true, true, true, false);
        runAlter("ALTER TABLE " + I_TABLE + " ADD PRIMARY KEY(id)");
        groupsMatch(C_NAME, A_NAME,  O_NAME, I_NAME);
    }

    //
    // DROP PRIMARY KEY
    //

    @Test
    public void dropPrimaryKey_C() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+C_TABLE+" DROP PRIMARY KEY");
        groupsDiffer(C_NAME, A_NAME, O_NAME, I_NAME);
        groupsDiffer(A_NAME, O_NAME, I_NAME);
        groupsMatch(O_NAME, I_NAME);
        checkIndexesInstead(C_NAME, "cc");
    }

    @Test
    public void dropPrimaryKey_A() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+A_TABLE+" DROP PRIMARY KEY");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        checkIndexesInstead(A_NAME, "aa");
    }

    @Test
    public void dropPrimaryKey_O() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+O_TABLE+" DROP PRIMARY KEY");
        groupsDiffer(I_NAME, C_NAME, A_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
        checkIndexesInstead(O_NAME, "oo");
    }

    @Test
    public void dropPrimaryKey_I() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+I_TABLE+" DROP PRIMARY KEY");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        checkIndexesInstead(I_NAME, "ii");
    }

    //
    // ADD GROUPING FOREIGN KEY
    //

    @Test
    public void addGroupingForeignKey_C() {
        String xTable = "x";
        TableName xName = new TableName(SCHEMA, xTable);
        int xid = createTable(xName, "id varchar(5) not null primary key");
        writeRows(
                row(xid, "1"), // Adopts 1 (well formed group)
                                        // Leave 2 orphan
                row(xid, "4"), // Adopts 2 (c has no children)
                row(xid, "5")  // No Children
        );
        createAndLoadCAOI_FK(true, true, true);
        runAlter("ALTER TABLE " + C_TABLE + " ADD GROUPING FOREIGN KEY(cc) REFERENCES x(id)");
        groupsMatch(xName, C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void addGroupingForeignKey_A() {
        createAndLoadCAOI_FK(false, true, true);
        runAlter("ALTER TABLE " + A_TABLE + " ADD GROUPING FOREIGN KEY(id) REFERENCES c(id)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void addGroupingForeignKey_O() {
        createAndLoadCAOI_FK(true, false, true);
        runAlter("ALTER TABLE " + O_TABLE + " ADD GROUPING FOREIGN KEY(id) REFERENCES c(id)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void addGroupingForeignKey_I() {
        createAndLoadCAOI_FK(true, true, false);
        runAlter("ALTER TABLE "+I_TABLE+" ADD GROUPING FOREIGN KEY(id) REFERENCES o(id)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    //
    // DROP GROUPING FOREIGN KEY
    //

    @Test
    public void dropGroupingForeignKey_A() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+A_TABLE+" DROP GROUPING FOREIGN KEY");
        groupsDiffer(C_NAME, A_NAME);
        groupsMatch(C_NAME, O_NAME, I_NAME);
    }

    @Test
    public void dropGroupingForeignKey_O() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+O_TABLE+" DROP GROUPING FOREIGN KEY");
        groupsDiffer(C_NAME, O_NAME);
        groupsMatch(O_NAME, I_NAME);
        groupsMatch(C_NAME, A_NAME);
    }

    @Test
    public void dropGroupingForeignKey_I() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + I_TABLE + " DROP GROUPING FOREIGN KEY");
        groupsDiffer(O_NAME, I_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
    }

    //
    // RENAME TABLE
    //

    @Test
    public void renameTable_C_X() {
        createAndLoadCAOI();
        runRenameTable(C_NAME, X_NAME);
        groupsMatch(X_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void renameTable_A_X() {
        createAndLoadCAOI();
        runRenameTable(A_NAME, X_NAME);
        groupsMatch(C_NAME, X_NAME, O_NAME, I_NAME);
    }

    @Test
    public void renameTable_O_X() {
        createAndLoadCAOI();
        runRenameTable(O_NAME, X_NAME);
        groupsMatch(C_NAME, A_NAME, X_NAME, I_NAME);
    }

    @Test
    public void renameTable_I_X() {
        createAndLoadCAOI();
        runRenameTable(I_NAME, X_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME, X_NAME);
    }

    //
    // RENAME COLUMN <parent pk and fk>
    //

    @Test
    public void renameColumn_C_id() {
        createAndLoadCAOI();
        runRenameColumn(C_NAME, "id", "di");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void renameColumn_A_id() {
        createAndLoadCAOI();
        runRenameColumn(A_NAME, "id", "di");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void renameColumn_O_id() {
        createAndLoadCAOI();
        runRenameColumn(O_NAME, "id", "di");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void renameColumn_I_id() {
        createAndLoadCAOI();
        runRenameColumn(I_NAME, "id", "di");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }
}
