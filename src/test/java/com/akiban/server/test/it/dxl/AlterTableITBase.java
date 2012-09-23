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

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AISMerge;
import com.akiban.ais.model.AISTableNameChanger;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.TableChange;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.test.it.qp.TestRow;
import com.akiban.sql.StandardException;
import com.akiban.sql.aisddl.AlterTableDDL;
import com.akiban.sql.parser.AlterTableNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AlterTableITBase extends ITBase {
    private final static String EXPECTED_VOLUME_NAME = "akiban_data";

    protected static final String SCHEMA = "test";
    protected static final String X_TABLE = "x";
    protected static final String C_TABLE = "c";
    protected static final String O_TABLE = "o";
    protected static final String I_TABLE = "i";
    protected static final TableName X_NAME = new TableName(SCHEMA, X_TABLE);
    protected static final TableName C_NAME = new TableName(SCHEMA, C_TABLE);
    protected static final TableName O_NAME = new TableName(SCHEMA, O_TABLE);
    protected static final TableName I_NAME = new TableName(SCHEMA, I_TABLE);
    protected static final List<TableChange> NO_CHANGES = Collections.emptyList();

    protected Map<Integer,List<String>> checkedIndexes = new HashMap<Integer, List<String>>();

    // Workaround for bug1052594 (Persistit brings trees back to life, this deletes data dir)
    @Override
    public Collection<Property> startupConfigProperties() {
        return uniqueStartupConfigProperties(AlterTableITBase.class);
    }

    @After
    public void lookForCheckedIndexes() {
        for(Map.Entry<Integer, List<String>> entry : checkedIndexes.entrySet()) {
            List<String> value = entry.getValue();
            expectIndexes(entry.getKey(), value.toArray(new String[value.size()]));
        }
        checkedIndexes.clear();
    }

    // Added after bug1047977
    @After
    public void lookForDanglingTrees() throws Exception {
        // Collect all trees Persistit currently has
        Set<String> storageTrees = new TreeSet<String>();
        storageTrees.addAll(Arrays.asList(treeService().getDb().getVolume(EXPECTED_VOLUME_NAME).getTreeNames()));

        // Collect all trees in AIS
        Set<String> knownTrees = AISMerge.computeTreeNames(ddl().getAIS(session()));
        knownTrees.add(TreeService.SCHEMA_TREE_NAME); // Used by SchemaManager

        // Any difference is an error
        Set<String> difference = new TreeSet<String>(storageTrees);
        difference.removeAll(knownTrees);

        assertEquals("Found orphaned trees", "[]", difference.toString());
    }

    protected void checkIndexesInstead(TableName name, String... indexNames) {
        checkedIndexes.put(tableId(name), Arrays.asList(indexNames));
    }

    protected QueryContext queryContext() {
        return null; // Not needed
    }

    protected ChangeLevel getDefaultChangeLevel() {
        return ChangeLevel.TABLE;
    }

    protected DDLFunctions ddlForAlter() {
        return ddl();
    }

    protected void runAlter(String sql) {
        runAlter(getDefaultChangeLevel(), sql);
    }

    protected void runAlter(ChangeLevel expectedChangeLevel, String sql) {
        SQLParser parser = new SQLParser();
        StatementNode node;
        try {
            node = parser.parseStatement(sql);
        } catch(StandardException e) {
            throw new RuntimeException(e);
        }
        assertTrue("is alter node", node instanceof AlterTableNode);
        ChangeLevel level = AlterTableDDL.alterTable(ddlForAlter(), dml(), session(), SCHEMA, (AlterTableNode) node, queryContext());
        assertEquals("ChangeLevel", expectedChangeLevel, level);
        updateAISGeneration();
    }

    protected void runAlter(ChangeLevel expectedChangeLevel, TableName name, UserTable newDefinition,
                            List<TableChange> columnChanges, List<TableChange> indexChanges) {
        ChangeLevel actual = ddlForAlter().alterTable(session(), name, newDefinition, columnChanges, indexChanges, queryContext());
        assertEquals("ChangeLevel", expectedChangeLevel, actual);
        updateAISGeneration();
    }

    protected void runRenameTable(TableName oldName, TableName newName) {
        AkibanInformationSchema aisCopy = AISCloner.clone(ddl().getAIS(session()));
        UserTable oldTable = aisCopy.getUserTable(oldName);
        assertNotNull("Found old table " + oldName, oldTable);
        AISTableNameChanger changer = new AISTableNameChanger(aisCopy.getUserTable(oldName), newName);
        changer.doChange();
        UserTable newTable = aisCopy.getUserTable(newName);
        assertNotNull("Found new table " + newName, oldTable);
        runAlter(ChangeLevel.METADATA,
                 oldName, newTable, NO_CHANGES, NO_CHANGES);
    }

    protected void runRenameColumn(TableName tableName, String oldColName, String newColName) {
        AkibanInformationSchema aisCopy = AISCloner.clone(ddl().getAIS(session()));
        UserTable tableCopy = aisCopy.getUserTable(tableName);
        assertNotNull("Found table " + tableName, tableCopy);
        Column oldColumn = tableCopy.getColumn(oldColName);
        assertNotNull("Found old column " + oldColName, oldColumn);

        // Have to do this manually as parser doesn't support it, duplicates much of the work in AlterTableDDL
        List<Column> columns = new ArrayList<Column>(tableCopy.getColumns());
        tableCopy.dropColumns();
        for(Column column : columns) {
            Column.create(tableCopy, column, (column == oldColumn) ? newColName : null, null);
        }

        Column newColumn = tableCopy.getColumn(newColName);
        assertNotNull("Found new column " + newColName, newColumn);

        List<TableIndex> indexes = new ArrayList<TableIndex>(tableCopy.getIndexes());
        for(TableIndex index : indexes) {
            if(index.containsTableColumn(tableName, oldColName)) {
                tableCopy.removeIndexes(Collections.singleton(index));
                TableIndex indexCopy = TableIndex.create(tableCopy, index);
                for(IndexColumn iCol : index.getKeyColumns()) {
                    IndexColumn.create(indexCopy, (iCol.getColumn() == oldColumn) ? newColumn : iCol.getColumn(), iCol, iCol.getPosition());
                }
            }
        }

        runAlter(ChangeLevel.METADATA,
                 tableName, tableCopy, Arrays.asList(TableChange.createModify(oldColName, newColName)), NO_CHANGES);
    }

    protected RowBase testRow(RowType type, Object... fields) {
        return new TestRow(type, fields);
    }

    protected void createAndLoadCAOI_PK_FK(boolean cPK, boolean aPK, boolean aFK, boolean oPK, boolean oFK, boolean iPK, boolean iFK) {
        throw new UnsupportedOperationException();
    }

    protected final void createAndLoadCAOI() {
        createAndLoadCAOI_PK_FK(true, true, true, true, true, true, true);
    }

    protected final void createAndLoadCAOI_PK(boolean cPK, boolean aPK, boolean oPK, boolean iPK) {
        createAndLoadCAOI_PK_FK(cPK, aPK, true, oPK, true, iPK, true);
    }

    protected final void createAndLoadCAOI_FK(boolean aFK, boolean oFK, boolean iFK) {
        createAndLoadCAOI_PK_FK(true, true, aFK, true, oFK, true, iFK);
    }


    // Note: Does not handle null index contents, check manually in that case
    private static class SingleColumnComparator implements Comparator<NewRow> {
        private final int colPos;

        SingleColumnComparator(int colPos) {
            this.colPos = colPos;
        }

        @Override
        public int compare(NewRow o1, NewRow o2) {
            Object col1 = o1.get(colPos);
            Object col2 = o2.get(colPos);
            if(col1 == null && col2 == null) {
                return 0;
            }
            if(col1 == null) {
                return -1;
            }
            if(col2 == null) {
                return +1;
            }
            return ((Comparable)col1).compareTo(col2);
        }
    }

    private void checkIndexContents(int tableID) {
        if(tableID == 0) {
            return;
        }

        updateAISGeneration();
        AkibanInformationSchema ais = ddl().getAIS(session());
        UserTable table = ais.getUserTable(tableID);
        List<NewRow> tableRows = new ArrayList<NewRow>(scanAll(scanAllRequest(tableID, true)));

        for(TableIndex index : table.getIndexesIncludingInternal()) {
            if(index.getKeyColumns().size() == 1) {
                int colPos = index.getKeyColumns().get(0).getColumn().getPosition();
                Collections.sort(tableRows, new SingleColumnComparator(colPos));

                List<NewRow> indexRows = scanAllIndex(index);

                if(tableRows.size() != indexRows.size()) {
                    assertEquals(index + " size does not match table size",
                                 tableRows.toString(), indexRows.toString());
                }

                for(int i = 0; i < tableRows.size(); ++i) {
                    Object tableObj = tableRows.get(i).get(colPos);
                    Object indexObj = indexRows.get(i).get(colPos);
                    assertEquals(index + " contents mismatch at row " + i,
                                 tableObj, indexObj);
                }
            }
        }
    }

    @After
    public final void doCheckAllSingleColumnIndexes() {
        for(UserTable table : ddl().getAIS(session()).getUserTables().values()) {
            if(!TableName.INFORMATION_SCHEMA.equals(table.getName().getSchemaName())) {
                checkIndexContents(table.getTableId());
            }
        }
    }
}
