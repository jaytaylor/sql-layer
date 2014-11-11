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

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.server.error.NotNullViolationException;
import com.foundationdb.server.error.PrimaryKeyNullColumnException;

import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

import com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Group altering actions on standard CAOI schema.
 */
public class AlterTableCAOIIT extends AlterTableITBase {
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
        aid = createTable(A_NAME, "id int not null "+(aPK ? "primary key" : "")+", cid int, aa varchar(5)"+
                (aFK ? "," + akibanFK("cid", C_TABLE, "id") : ""));
        oid = createTable(O_NAME, "id int not null "+(oPK ? "primary key" : "")+", cid int, oo varchar(5)"+
                (oFK ? "," + akibanFK("cid", C_TABLE, "id") : ""));
        iid = createTable(I_NAME, "id int not null "+(iPK ? "primary key" : "")+", oid int, ii varchar(5)"+
                (iFK ? "," + akibanFK("oid", O_TABLE, "id") : ""));
        // Index fk column
        createIndex(SCHEMA, A_TABLE, "cid", "cid");
        createIndex(SCHEMA, O_TABLE, "cid", "cid");
        createIndex(SCHEMA, I_TABLE, "oid", "oid");
        // Index on non-pk, non-fk column
        createIndex(SCHEMA, C_TABLE, "cc", "cc");
        createIndex(SCHEMA, A_TABLE, "aa", "aa");
        createIndex(SCHEMA, O_TABLE, "oo", "oo");
        createIndex(SCHEMA, I_TABLE, "ii", "ii");
        // Default post indexes presence checking
        checkedIndexes.put(cid, Arrays.asList("PRIMARY", "cc"));
        checkedIndexes.put(aid, Arrays.asList("PRIMARY", "cid", "aa"));
        checkedIndexes.put(oid, Arrays.asList("PRIMARY", "cid", "oo"));
        checkedIndexes.put(iid, Arrays.asList("PRIMARY", "oid", "ii"));
        txnService().beginTransaction(session());
        // Data
        writeRows(
                row(cid, 1L, "1"),
                    row(aid, 10L, 1L, "11"),
                    row(oid, 10L, 1L, "11"),
                        row(iid, 100L, 10L, "110"),
                        row(iid, 101L, 10L, "111"),
                    row(oid, 11L, 1L, "12"),
                        row(iid, 111L, 11L, "122"),
                row(cid, 2L, "2"),                     // No children
                // No cust(3L)
                    row(oid, 30L, 3L, "33"),           // Level 1 orphan
                        row(iid, 300L, 30L, "330"),
                row(cid, 4L, "4"),
                    row(aid, 40L, 4L, "44"),
                    row(aid, 41L, 4L, "45"),
                    // No 40 order
                        row(iid, 400L, 40L, "440"),    // Level 2 orphan
                        row(iid, 401L, 40L, "441"),    // Level 2 orphan
                // No cust(5L)
                    row(aid, 50L, 5L, "55")            // Level 1 orphan
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
    // SET DATA TYPE, parent column
    //

    @Test
    public void setDataType_C_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + C_TABLE + " ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsDiffer(C_NAME, A_NAME, O_NAME, I_NAME);
        groupsDiffer(A_NAME, O_NAME, I_NAME);
        groupsMatch(O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {"1", "1"},
                   {"2", "2"},
                   {"4", "4"},
                },
                ais().getTable(C_NAME));
    }

    @Test
    public void setDataType_A_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + A_TABLE + " ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {"10", 1},
                   {"40", 4},
                   {"41", 4},
                   {"50", 5},
                },
                ais().getTable(A_NAME).getIndex("PRIMARY"));
    }

    @Test
    public void setDataType_O_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + O_TABLE + " ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsDiffer(I_NAME, C_NAME, A_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
    }

    @Test
    public void setDataType_I_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + I_TABLE + " ALTER COLUMN id SET DATA TYPE varchar(32)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    //
    // SET DATA TYPE, child column
    //

    @Test
    public void setDataType_A_cid() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+A_TABLE+" ALTER COLUMN cid SET DATA TYPE varchar(32)");
        groupsDiffer(C_NAME, A_NAME);
        groupsMatch(C_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {10, "1", "11"},
                   {40, "4", "44"},
                   {41, "4", "45"},
                   {50, "5", "55"},
                },
                ais().getTable(A_NAME));
    }

    @Test
    public void setDataType_O_cid() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + O_TABLE + " ALTER COLUMN cid SET DATA TYPE varchar(32)");
        groupsDiffer(C_NAME, O_NAME);
        groupsMatch(C_NAME, A_NAME);
        groupsMatch(O_NAME, I_NAME);
    }

    @Test
    public void setDataType_I_oid() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + I_TABLE + " ALTER COLUMN oid SET DATA TYPE varchar(32)");
        groupsDiffer(O_NAME, I_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
    }

    //
    // NULL <parent pk>
    //

    @Test
    public void setNull_C_id() {
        createAndLoadCAOI();
        try {
            runAlter("ALTER TABLE " + C_TABLE + " ALTER COLUMN id NULL");
            fail("Expected: PrimaryKeyNullColumnException");
        } catch(PrimaryKeyNullColumnException e) {
        }
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void setNull_A_id() {
        createAndLoadCAOI();
        try {
            runAlter("ALTER TABLE " + A_TABLE + " ALTER COLUMN id NULL");
            fail("Expected: PrimaryKeyNullColumnException");
        } catch(PrimaryKeyNullColumnException e) {
        }
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void setNull_O_id() {
        createAndLoadCAOI();
        try {
            runAlter("ALTER TABLE " + O_TABLE + " ALTER COLUMN id NULL");
            fail("Expected: PrimaryKeyNullColumnException");
        } catch(PrimaryKeyNullColumnException e) {
        }
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void setNull_I_id() {
        createAndLoadCAOI();
        try {
            runAlter("ALTER TABLE " + I_TABLE + " ALTER COLUMN id NULL");
            fail("Expected: PrimaryKeyNullColumnException");
        } catch(PrimaryKeyNullColumnException e) {
        }
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    //
    // NOT NULL <child fk>
    //

    @Test
    public void setNotNull_A_cid() {
        createAndLoadCAOI();
        runAlter(ChangeLevel.METADATA_CONSTRAINT, "ALTER TABLE " + A_TABLE + " ALTER COLUMN cid NOT NULL");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {10, 1},
                   {40, 4},
                   {41, 4},
                   {50, 5},
                },
                ais().getTable(A_NAME).getIndex("PRIMARY"));
    }

    @Test
    public void setNotNull_O_cid() {
        createAndLoadCAOI();
        runAlter(ChangeLevel.METADATA_CONSTRAINT, "ALTER TABLE " + O_TABLE + " ALTER COLUMN cid NOT NULL");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void setNotNull_I_oid() {
        createAndLoadCAOI();
        runAlter(ChangeLevel.METADATA_CONSTRAINT, "ALTER TABLE " + I_TABLE + " ALTER COLUMN oid NOT NULL");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    //
    // NOT NULL <child fk> (with failure)
    //

    @Test
    public void setNotNull_A_cid_failing() {
        createAndLoadCAOI();
        writeRow(aid, 1000L, null, "1000");
        try {
            runAlter("ALTER TABLE " + A_TABLE + " ALTER COLUMN cid NOT NULL");
            fail("Expected NotNullViolationException");
        } catch(NotNullViolationException e) {
        }
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void setNotNull_O_cid_failing() {
        createAndLoadCAOI();
        writeRow(oid, 1000L, null, "1000");
        try {
            runAlter("ALTER TABLE " + O_TABLE + " ALTER COLUMN cid NOT NULL");
            fail("Expected NotNullViolationException");
        } catch(NotNullViolationException e) {
        }
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void setNotNull_I_oid_failing() {
        createAndLoadCAOI();
        writeRow(iid, 1000L, null, "1000");
        try {
            runAlter("ALTER TABLE " + I_TABLE + " ALTER COLUMN oid NOT NULL");
            fail("Expected NotNullViolationException");
        } catch(NotNullViolationException e) {
        }
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    //
    // DROP COLUMN <parent pk>
    //

    @Test
    public void dropColumn_C_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+C_TABLE+" DROP COLUMN id");
        groupsDiffer(C_NAME, A_NAME, O_NAME, I_NAME);
        groupsDiffer(A_NAME, O_NAME, I_NAME);
        groupsMatch(O_NAME, I_NAME);
        checkIndexesInstead(C_NAME, "cc");
        compareRows(
                new Object[][] {
                   {"1", 1},
                   {"2", 2},
                   {"4", 3},
                },
                ais().getTable(C_NAME));
    }

    @Test
    public void dropColumn_A_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+A_TABLE+" DROP COLUMN id");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        checkIndexesInstead(A_NAME, "cid", "aa");
        compareRows(
                new Object[][] {
                   {1, "11", 1},
                   {4, "44", 2},
                   {4, "45", 3},
                   {5, "55", 4},
                },
                ais().getTable(A_NAME));
    }

    @Test
    public void dropColumn_O_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+O_TABLE+" DROP COLUMN id");
        groupsDiffer(I_NAME, C_NAME, A_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
        checkIndexesInstead(O_NAME, "cid", "oo");
    }

    @Test
    public void dropColumn_I_id() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+I_TABLE+" DROP COLUMN id");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        checkIndexesInstead(I_NAME, "oid", "ii");
    }

    //
    // DROP COLUMN <child fk>
    //

    @Test
    public void dropColumn_A_cid() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + A_TABLE + " DROP COLUMN cid");
        groupsDiffer(C_NAME, A_NAME);
        groupsMatch(C_NAME, O_NAME, I_NAME);
        checkIndexesInstead(A_NAME, "PRIMARY", "aa");
        compareRows(
                new Object[][] {
                   {10, "11"},
                   {40, "44"},
                   {41, "45"},
                   {50, "55"},
                },
                ais().getTable(A_NAME));
    }

    @Test
    public void dropColumn_O_cid() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + O_TABLE + " DROP COLUMN cid");
        groupsDiffer(C_NAME, O_NAME);
        groupsMatch(C_NAME, A_NAME);
        groupsMatch(O_NAME, I_NAME);
        checkIndexesInstead(O_NAME, "PRIMARY", "oo");
    }

    @Test
    public void dropColumn_I_oid() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE " + I_TABLE + " DROP COLUMN oid");
        groupsDiffer(O_NAME, I_NAME);
        groupsMatch(C_NAME, A_NAME,  O_NAME);
        checkIndexesInstead(I_NAME, "PRIMARY", "ii");
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
        compareRows(
                new Object[][] {
                   {1, "1", 1},
                   {2, "2", 2},
                   {4, "4", 3},
                },
                ais().getTable(C_NAME));
    }

    @Test
    public void dropPrimaryKey_A() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+A_TABLE+" DROP PRIMARY KEY");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        checkIndexesInstead(A_NAME, "cid", "aa");
        compareRows(
                new Object[][] {
                   {10, 1, "11", 1},
                   {40, 4, "44", 2},
                   {41, 4, "45", 3},
                   {50, 5, "55", 4},
                },
                ais().getTable(A_NAME));
    }

    @Test
    public void dropPrimaryKey_O() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+O_TABLE+" DROP PRIMARY KEY");
        groupsDiffer(I_NAME, C_NAME, A_NAME);
        groupsMatch(C_NAME, A_NAME, O_NAME);
        checkIndexesInstead(O_NAME, "cid", "oo");
    }

    @Test
    public void dropPrimaryKey_I() {
        createAndLoadCAOI();
        runAlter("ALTER TABLE "+I_TABLE+" DROP PRIMARY KEY");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        checkIndexesInstead(I_NAME, "oid", "ii");
    }

    //
    // ADD GROUPING FOREIGN KEY
    //

    @Test
    public void addGroupingForeignKey_C() {
        int xid = createTable(X_NAME, "id varchar(5) not null primary key");
        writeRows(
                row(xid, "1"), // Adopts 1 (well formed group)
                                        // Leave 2 orphan
                row(xid, "4"), // Adopts 2 (c has no children)
                row(xid, "5")  // No Children
        );
        createAndLoadCAOI_FK(true, true, true);
        runAlter("ALTER TABLE " + C_TABLE + " ADD GROUPING FOREIGN KEY(cc) REFERENCES x(id)");
        groupsMatch(X_NAME, C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void addGroupingForeignKey_A() {
        createAndLoadCAOI_FK(false, true, true);
        runAlter("ALTER TABLE " + A_TABLE + " ADD GROUPING FOREIGN KEY(cid) REFERENCES c(id)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void addGroupingForeignKey_O() {
        createAndLoadCAOI_FK(true, false, true);
        runAlter("ALTER TABLE " + O_TABLE + " ADD GROUPING FOREIGN KEY(cid) REFERENCES c(id)");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void addGroupingForeignKey_I() {
        createAndLoadCAOI_FK(true, true, false);
        runAlter("ALTER TABLE "+I_TABLE+" ADD GROUPING FOREIGN KEY(oid) REFERENCES o(id)");
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
        //runRenameTable(C_NAME, X_NAME);
        runAlter (ChangeLevel.METADATA, "ALTER TABLE " + C_NAME + " RENAME TO " + X_NAME);
        groupsMatch(X_NAME, A_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {1, "1"},
                   {2, "2"},
                   {4, "4"},
                },
                ais().getTable(X_NAME));
    }

    @Test
    public void renameTable_A_X() {
        createAndLoadCAOI();
        //runRenameTable(A_NAME, X_NAME);
        runAlter(ChangeLevel.METADATA, "ALTER TABLE " + A_NAME + " RENAME TO " + X_NAME);
        groupsMatch(C_NAME, X_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {10, 1, "11"},
                   {40, 4, "44"},
                   {41, 4, "45"},
                   {50, 5, "55"},
                },
                ais().getTable(X_NAME));
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
    // RENAME COLUMN <parent pk>
    //

    @Test
    public void renameColumn_C_id() {
        createAndLoadCAOI();
        runAlter(ChangeLevel.METADATA, "ALTER TABLE " + C_NAME + " RENAME COLUMN id TO di");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {1, "1"},
                   {2, "2"},
                   {4, "4"},
                },
                ais().getTable(C_NAME));
    }

    @Test
    public void renameColumn_A_id() {
        createAndLoadCAOI();
        runAlter(ChangeLevel.METADATA, "ALTER TABLE " + A_NAME + " RENAME COLUMN id to di");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        
        compareRows(
                new Object[][] {
                   {10, 1, "11"},
                   {40, 4, "44"},
                   {41, 4, "45"},
                   {50, 5, "55"},
                },
                ais().getTable(A_NAME));
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

    //
    // RENAME COLUMN <child fk>
    //

    @Test
    public void renameColumn_A_cid() {
        createAndLoadCAOI();
        runAlter(ChangeLevel.METADATA, "ALTER TABLE " + A_NAME + " RENAME COLUMN cid to dic");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {10, 1, "11"},
                   {40, 4, "44"},
                   {41, 4, "45"},
                   {50, 5, "55"},
                },
                ais().getTable(A_NAME));
    }

    @Test
    public void renameColumn_O_cid() {
        createAndLoadCAOI();
        runRenameColumn(O_NAME, "cid", "dic");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }

    @Test
    public void renameColumn_I_oid() {
        createAndLoadCAOI();
        runRenameColumn(I_NAME, "oid", "dio");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
    }
    
    //
    // COLUMN DEFAULT 0
    //
    @Test
    public void alterDefault_C_id() {
        createAndLoadCAOI();
        runAlter(ChangeLevel.METADATA, "ALTER TABLE " + C_NAME + " ALTER COLUMN id DEFAULT 0");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {1, "1"},
                   {2, "2"},
                   {4, "4"},
                },
                ais().getTable(C_NAME));
    }
    
    @Test
    public void alterDefault_A_id() {
        createAndLoadCAOI();
        runAlter(ChangeLevel.METADATA, "ALTER TABLE " + A_NAME + " ALTER COLUMN id DEFAULT 0");
        groupsMatch(C_NAME, A_NAME, O_NAME, I_NAME);
        compareRows(
                new Object[][] {
                   {10, 1, "11"},
                   {40, 4, "44"},
                   {41, 4, "45"},
                   {50, 5, "55"},
                },
                ais().getTable(A_NAME));
    }

    //
    // Affected group indexes
    //

    @Test
    public void dropColumn_Multi_GI() {
        createAndLoadCAOI();
        // Gets dropped completely
        createFromDDL(SCHEMA, "CREATE INDEX cc_oo ON i(c.cc, o.oo) USING LEFT JOIN");
        // Get recreated
        createFromDDL(SCHEMA, "CREATE INDEX oo_ii_cc ON i(o.oo, i.ii, c.cc) USING LEFT JOIN");
        runAlter(ChangeLevel.TABLE, "ALTER TABLE "+O_TABLE+" DROP COLUMN oo");
        checkIndexesInstead(O_NAME, "PRIMARY", "cid");
        compareRows(
            new Object[][] {
                { "110", "1", "1", 10, 100 },
                { "111", "1", "1", 10, 101 },
                { "122", "1", "1", 11, 111 }
            },
            ais().getGroup(C_NAME).getIndex("oo_ii_cc")
        );
    }

    @Test
    public void setDataType_Multi_GI() {
        createAndLoadCAOI();
        // All tables are included in at least one GI
        createFromDDL(SCHEMA, "CREATE INDEX aa_cc ON a(a.aa, c.cc) USING LEFT JOIN");
        createFromDDL(SCHEMA, "CREATE INDEX ii_oo ON i(i.ii, o.oo) USING LEFT JOIN");
        // Doesn't affect ii_oo
        runAlter(ChangeLevel.TABLE, "ALTER TABLE "+A_TABLE+" ALTER COLUMN aa SET DATA TYPE char(5)");
        // Doesn't affect aa_cc
        runAlter(ChangeLevel.TABLE, "ALTER TABLE "+I_TABLE+" ALTER COLUMN ii SET DATA TYPE char(5)");
        compareRows(
            new Object[][] {
                { "11", "1", 1, 10 },
                { "44", "4", 4, 40 },
                { "45", "4", 4, 41 }
            },
            ais().getGroup(C_NAME).getIndex("aa_cc")
        );
        compareRows(
            new Object[][] {
                { "110", "11", 1, 10, 100 },
                { "111", "11", 1, 10, 101 },
                { "122", "12", 1, 11, 111 },
                { "330", "33", 3, 30, 300 }
            },
            ais().getGroup(C_NAME).getIndex("ii_oo")
        );
    }

    @Test
    public void addColumn_C_GI() {
        addColCheckGIs(C_TABLE, "new_col");
    }

    @Test
    public void addColumn_A_GI() {
        addColCheckGIs(A_TABLE, "new_col");
    }

    @Test
    public void addColumn_O_GI() {
        addColCheckGIs(O_TABLE, "new_col");
    }

    @Test
    public void addColumn_I_GI() {
        addColCheckGIs(I_TABLE, "new_col");
    }

    //
    // Rollback testing
    //

    @Test
    public void groupingChangeWithFailureInMiddle() {
        // "Worst" case scenario, grouping change almost complete and then failure
        // Create I un-grouped, add GFK to O and change oid to NOT NULL in same alter (with a null row)

        createAndLoadCAOI_FK(true, true, false);
        writeRow(iid, 1000L, null, "1000");

        TestAISBuilder builder = new TestAISBuilder(typesRegistry());
        // stub parent
        builder.table(SCHEMA, O_TABLE);
        builder.column(SCHEMA, O_TABLE, "id", 0, "MCOMPAT", "int", false);
        // Changed child
        builder.table(SCHEMA, I_TABLE);
        builder.column(SCHEMA, I_TABLE, "id", 0, "MCOMPAT", "int", false);
        builder.column(SCHEMA, I_TABLE, "oid", 1, "MCOMPAT", "int", false);
        builder.column(SCHEMA, I_TABLE, "ii", 2, "MCOMPAT", "varchar", 5L, null, true);
        builder.pk(SCHEMA, I_TABLE);
        builder.indexColumn(SCHEMA, I_TABLE, Index.PRIMARY, "id", 0, true, null);
        builder.index(SCHEMA, I_TABLE, "oid");
        builder.indexColumn(SCHEMA, I_TABLE, "oid", "oid", 0, true, null);
        builder.index(SCHEMA, I_TABLE, "ii");
        builder.indexColumn(SCHEMA, I_TABLE, "ii", "ii", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup(O_TABLE, SCHEMA);
        builder.addTableToGroup(O_TABLE, SCHEMA, O_TABLE);
        builder.joinTables("o/i", SCHEMA, O_TABLE, SCHEMA, I_TABLE);
        builder.joinColumns("o/i", SCHEMA, O_TABLE, "id", SCHEMA, I_TABLE, "oid");
        builder.addJoinToGroup(O_TABLE, "o/i", 0);
        builder.groupingIsComplete();

        try {
            Table newDef = builder.akibanInformationSchema().getTable(I_NAME);
            runAlter(ChangeLevel.GROUP, I_NAME, newDef, Arrays.asList(TableChange.createModify("oid", "oid")), NO_CHANGES);
            fail("Expected NotNullViolationException");
        } catch(NotNullViolationException e) {
        }
    }


    private void addColCheckGIs(String table, String col) {
        createAndLoadCAOI();
        // All tables are included in at least one GI
        createFromDDL(SCHEMA, "CREATE INDEX aa_cc ON a(a.aa, c.cc) USING LEFT JOIN");
        createFromDDL(SCHEMA, "CREATE INDEX ii_oo ON i(i.ii, o.oo) USING LEFT JOIN");
        runAlter(ChangeLevel.TABLE, "ALTER TABLE "+table+" ADD COLUMN "+col+" INT");
        compareRows(
            new Object[][] {
                { "11", "1", 1, 10 },
                { "44", "4", 4, 40 },
                { "45", "4", 4, 41 }
            },
            ais().getGroup(C_NAME).getIndex("aa_cc")
        );
        compareRows(
            new Object[][] {
                { "110", "11", 1, 10, 100 },
                { "111", "11", 1, 10, 101 },
                { "122", "12", 1, 11, 111 },
                { "330", "33", 3, 30, 300 }
            },
            ais().getGroup(C_NAME).getIndex("ii_oo")
        );
    }
}
