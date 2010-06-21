package com.akiban.cserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

/**
 * Caches RowDef instances. In this incarnation, this class also constructs
 * RowDef objects from the AkibaInformationSchema. The translation is done in
 * the {@link #setAIS(AkibaInformationSchema)} method.
 * 
 * @author peter
 */
public class RowDefCache implements CServerConstants {

    private static final Log LOG = LogFactory.getLog(RowDefCache.class
            .getName());

    private final Map<Integer, RowDef> cacheMap = new TreeMap<Integer, RowDef>();

    private final Map<String, Integer> nameMap = new TreeMap<String, Integer>();

    /**
     * Look up and return a RowDef for a supplied rowDefId value.
     * 
     * @param rowDefId
     * @return the corresponding RowDef
     */
    public synchronized RowDef getRowDef(final int rowDefId) {
        RowDef rowDef = cacheMap.get(Integer.valueOf(rowDefId));
        if (rowDef == null) {
            rowDef = lookUpRowDef(rowDefId);
            cacheMap.put(Integer.valueOf(rowDefId), rowDef);
        }
        return rowDef;
    }

    public synchronized List<RowDef> getRowDefs() {
        return new ArrayList<RowDef>(cacheMap.values());
    }

    public synchronized RowDef getRowDef(final String tableName) {
        final Integer key = nameMap.get(tableName);
        if (key == null) {
            return null;
        }
        return getRowDef(key.intValue());
    }

    public synchronized void clear() {
        cacheMap.clear();
        nameMap.clear();
    }

    /**
     * Receive an instance of the AkibaInformationSchema, crack it and produce
     * the RowDef instances it defines.
     * 
     * @param ais
     */
    public synchronized void setAIS(final AkibaInformationSchema ais) {
        for (final UserTable table : ais.getUserTables().values()) {
            putRowDef(createUserTableRowDef(ais, table));
        }

        for (final GroupTable table : ais.getGroupTables().values()) {
            putRowDef(createGroupTableRowDef(ais, table));
        }

        analyzeAll();

        if (LOG.isDebugEnabled()) {
            LOG.debug(toString());
        }

    }

    private FieldDef fieldDef(final Column column) {
        return new FieldDef(column.getName(), column.getType(), column
                .getMaxStorageSize().intValue(), column.getPrefixSize()
                .intValue());
    }

    private RowDef createUserTableRowDef(final AkibaInformationSchema ais,
            final UserTable table) {

        // rowDefId
        final int rowDefId = table.getTableId();
        int autoIncrementField = -1;

        // FieldDef[]
        final FieldDef[] fieldDefs = new FieldDef[table.getColumns().size()];
        for (final Column column : table.getColumns()) {
            final int fieldIndex = column.getPosition();
            fieldDefs[fieldIndex] = fieldDef(column);
            if (column.getInitialAutoIncrementValue() != null
                    && column.getInitialAutoIncrementValue() > 0) {
                autoIncrementField = fieldIndex;
            }
        }

        // parentRowDef
        int parentRowDefId;
        int[] parentJoinFields;
        if (table.getParentJoin() != null) {
            final Join join = table.getParentJoin();
            final UserTable parentTable = join.getParent();
            parentRowDefId = parentTable.getTableId();
            //
            // parentJoinFields - TODO - not sure this is right.
            //
            parentJoinFields = new int[join.getJoinColumns().size()];
            for (int index = 0; index < join.getJoinColumns().size(); index++) {
                final JoinColumn joinColumn = join.getJoinColumns().get(index);
                parentJoinFields[index] = joinColumn.getChild().getPosition();
            }
        } else {
            parentRowDefId = 0;
            parentJoinFields = new int[0];
        }

        // pkFields - The columns from this table, contributing to the hkey,
        // that don't have matching
        // columns in the parent.
        int[] pkFields = null;
        for (final Index index : table.getIndexes()) {
            if (!index.isPrimaryKey()) {
                continue;
            }
            if (pkFields != null) {
                throw new IllegalStateException(
                        "Can't handle two PK indexes on "
                                + table.getName().getTableName());
            }
            final List<IndexColumn> indexColumns = index.getColumns();
            final List<Integer> pkFieldList = new ArrayList<Integer>(1);
            for (final IndexColumn indexColumn : indexColumns) {
                final int position = indexColumn.getColumn().getPosition();
                boolean isParentJoin = false;
                for (int i = 0; i < parentJoinFields.length; i++) {
                    if (position == parentJoinFields[i]) {
                        isParentJoin = true;
                        break;
                    }
                }
                if (!isParentJoin) {
                    pkFieldList.add(position);
                }
            }

            int pkField = 0;
            pkFields = new int[pkFieldList.size()];
            for (final Integer position : pkFieldList) {
                pkFields[pkField++] = position;
            }
        }

        // root table
        UserTable root = table;
        while (root.getParentJoin() != null) {
            root = root.getParentJoin().getParent();
        }

        // group table name
        String groupTableName = null;
        for (final GroupTable groupTable : ais.getGroupTables().values()) {
            if (groupTable.getRoot().equals(root)) {
                groupTableName = groupTable.getName().getTableName();
            }
        }

        final RowDef rowDef = new RowDef(rowDefId, fieldDefs);

        // Secondary indexes
        final List<IndexDef> indexDefList = new ArrayList<IndexDef>();
        for (final Index index : table.getIndexes()) {
            final List<IndexColumn> indexColumns = index.getColumns();
            final List<Integer> indexColumnList = new ArrayList<Integer>(1);
            for (final IndexColumn indexColumn : indexColumns) {
                final int position = indexColumn.getColumn().getPosition();
                indexColumnList.add(position);
            }

            int columnIndex = 0;
            int[] indexFields = new int[indexColumnList.size()];
            for (final Integer position : indexColumnList) {
                indexFields[columnIndex++] = position;
            }

            final String treeName = groupTableName + "$$" + index.getIndexId();
            final IndexDef indexDef = new IndexDef(index.getIndexNameObject()
                    .getName(), rowDef, treeName, index.getIndexId(),
                    indexFields, index.isPrimaryKey(), index.isUnique());
            if (index.isPrimaryKey()) {
                indexDefList.add(0, indexDef);
            } else {
                indexDefList.add(indexDef);
            }
        }
        rowDef.setTableName(table.getName().getTableName());
        rowDef.setTreeName(groupTableName);
        rowDef.setSchemaName(table.getName().getSchemaName());
        rowDef.setPkFields(pkFields);
        rowDef.setParentRowDefId(parentRowDefId);
        rowDef.setParentJoinFields(parentJoinFields);
        rowDef.setIndexDefs(indexDefList.toArray(new IndexDef[indexDefList
                .size()]));
        rowDef.setOrdinal(0);
        rowDef.setAutoIncrementField(autoIncrementField);

        return rowDef;

    }

    private RowDef createGroupTableRowDef(final AkibaInformationSchema ais,
            final GroupTable table) {
        // rowDefId
        final int rowDefId = table.getTableId();

        // FieldDef[]
        final FieldDef[] fieldDefs = new FieldDef[table.getColumns().size()];
        final Column[] columns = new Column[table.getColumns().size()];
        int[] tempRowDefIds = new int[columns.length];
        int userTableIndex = 0;
        int columnCount = 0;
        for (final Column column : table.getColumns()) {
            final int p = column.getPosition();
            if (columns[p] != null) {
                throw new IllegalStateException("Group table column "
                        + column.getName() + " has overlapping position " + p);
            }
            columns[p] = column;
        }
        for (int position = 0; position < columns.length; position++) {
            final Column column = columns[position];
            final int fieldIndex = column.getPosition();
            fieldDefs[fieldIndex] = fieldDef(column);
            final Column userColumn = column.getUserColumn();
            if (userColumn.getPosition() == 0) {
                int userRowDefId = userColumn.getTable().getTableId();
                tempRowDefIds[userTableIndex] = userRowDefId;
                userTableIndex++;
                columnCount += userColumn.getTable().getColumns().size();
                RowDef userRowDef = cacheMap.get(Integer.valueOf(userRowDefId));
                userRowDef.setGroupRowDefId(rowDefId);
                userRowDef.setColumnOffset(position);
            }
        }
        final RowDef[] userTableRowDefs = new RowDef[userTableIndex];
        for (int index = 0; index < userTableIndex; index++) {
            userTableRowDefs[index] = cacheMap.get(Integer
                    .valueOf(tempRowDefIds[index]));
        }

        final RowDef rowDef = new RowDef(rowDefId, fieldDefs);
        final String groupTableName = table.getName().getTableName();

        // Secondary indexes
        final List<IndexDef> indexDefList = new ArrayList<IndexDef>();
        for (final Index index : table.getIndexes()) {
            final List<IndexColumn> indexColumns = index.getColumns();
            final List<Integer> indexColumnList = new ArrayList<Integer>(1);
            for (final IndexColumn indexColumn : indexColumns) {
                final int position = indexColumn.getColumn().getPosition();
                indexColumnList.add(position);
            }

            int columnIndex = 0;
            int[] indexFields = new int[indexColumnList.size()];
            for (final Integer position : indexColumnList) {
                indexFields[columnIndex++] = position;
            }

            final String treeName = groupTableName + "$$" + index.getIndexId();
            final IndexDef indexDef = new IndexDef(index.getIndexNameObject()
                    .getName(), rowDef, treeName, index.getIndexId(),
                    indexFields, false, index.isUnique());
            indexDefList.add(indexDef);
        }
        rowDef.setTableName(groupTableName);
        rowDef.setTreeName(groupTableName);
        rowDef.setSchemaName(table.getName().getSchemaName());
        rowDef.setPkFields(new int[0]);
        rowDef.setUserTableRowDefs(userTableRowDefs);
        rowDef.setIndexDefs(indexDefList.toArray(new IndexDef[indexDefList
                .size()]));
        rowDef.setOrdinal(0);

        return rowDef;
    }

    RowDef lookUpRowDef(final int rowDefId) {
        return null;
    }

    /**
     * Adds a RowDef preemptively to the cache. This is intended primarily to
     * simplify unit tests.
     * 
     * @param rowDef
     */
    public synchronized void putRowDef(final RowDef rowDef) {
        final Integer key = Integer.valueOf(rowDef.getRowDefId());
        final String name = rowDef.getSchemaName() == null ? rowDef
                .getTableName() : rowDef.getSchemaName() + "."
                + rowDef.getTableName();
        if (cacheMap.containsKey(key) || nameMap.containsKey(name)) {
            throw new IllegalStateException("RowDef " + rowDef
                    + " already exists");
        }
        cacheMap.put(key, rowDef);
        nameMap.put(name, key);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("\n");
        for (Map.Entry<String, Integer> entry : nameMap.entrySet()) {
            final RowDef rowDef = cacheMap.get(entry.getValue());
            sb.append("   ");
            sb.append(rowDef);
            sb.append("\n");
        }
        return sb.toString();
    }

    public void analyzeAll() {
        for (final RowDef rowDef : cacheMap.values()) {
            analyze(rowDef);
        }
    }

    void analyze(final RowDef rowDef) {
        rowDef.computeRowDefType(this);
        rowDef.computeFieldAssociations(this);
    }
}
