package com.akiban.cserver;

import com.akiban.ais.model.*;
import com.akiban.cserver.store.PersistitStoreTableManager;
import com.akiban.cserver.store.TableStatus;
import com.akiban.cserver.util.RowDefNotFoundException;
import com.persistit.exception.PersistitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Caches RowDef instances. In this incarnation, this class also constructs
 * RowDef objects from the AkibaInformationSchema. The translation is done in
 * the {@link #setAIS(AkibaInformationSchema)} method.
 * 
 * @author peter
 */
public class RowDefCache implements CServerConstants {

    // TODO: For debugging - remove this
    private static volatile RowDefCache LATEST;

    private static final Log LOG = LogFactory.getLog(RowDefCache.class
            .getName());

    private final Map<Integer, RowDef> cacheMap = new TreeMap<Integer, RowDef>();

    private final Map<String, Integer> nameMap = new TreeMap<String, Integer>();

    private int hashCode;

    {
        LATEST = this;
    }

    public static RowDefCache latest()
    {
        return LATEST;
    }

    /**
     * Look up and return a RowDef for a supplied rowDefId value.
     * 
     * @param rowDefId
     * @return the corresponding RowDef
     */
    public synchronized RowDef getRowDef(final int rowDefId) throws RowDefNotFoundException {
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

    public synchronized RowDef getRowDef(final String tableName) throws RowDefNotFoundException {
        final Integer key = nameMap.get(tableName);
        if (key == null) {
            return null;
        }
        return getRowDef(key.intValue());
    }

    /**
     * Given a schema and table name, gets a string that uniquely identifies a table. This string can then be
     * passed to {@link #getRowDef(String)}.
     * @param schema the schema
     * @param table the table name
     * @return a unique form
     */
    public static String nameOf(String schema, String table) {
        assert schema != null;
        assert table != null;
        return schema + "." + table;
    }

    public synchronized void clear() {
        cacheMap.clear();
        nameMap.clear();
        hashCode = 0;
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
        hashCode = cacheMap.hashCode();
    }

    public synchronized void fixUpOrdinals(PersistitStoreTableManager tableManager) throws PersistitException
    {
        for (final RowDef groupRowDef : getRowDefs()) {
            if (groupRowDef.isGroupTable()) {
                // groupTable has no ordinal
                groupRowDef.setOrdinal(0);
                final HashSet<Integer> assigned = new HashSet<Integer>();
                // First pass: merge already assigned values
                for (final RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
                    final TableStatus tableStatus = tableManager.getTableStatus(userRowDef.getRowDefId());
                    if (tableStatus.getOrdinal() != 0 &&
                        userRowDef.getOrdinal() != 0 &&
                        tableStatus.getOrdinal() != userRowDef.getOrdinal()) {
                        throw new IllegalStateException(
                            String.format("Mismatched ordinals: %s and %s",
                                          userRowDef.getOrdinal(),
                                          tableStatus.getOrdinal()));
                    }
                    int ordinal = 0;
                    if (tableStatus.getOrdinal() != 0) {
                        ordinal = tableStatus.getOrdinal();
                        userRowDef.setOrdinal(ordinal);
                    } else if (userRowDef.getOrdinal() != 0 && tableStatus.getOrdinal() == 0) {
                        ordinal = userRowDef.getOrdinal();
                        tableStatus.setOrdinal(ordinal);
                    }
                    if (ordinal != 0 && !assigned.add(ordinal)) {
                        throw new IllegalStateException(
                            String.format("Non-unique ordinal value %s added to %s", ordinal, assigned));
                    }
                }
                int nextOrdinal = 1;
                for (final RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
                    final TableStatus tableStatus = tableManager.getTableStatus(userRowDef.getRowDefId());
                    if (userRowDef.getOrdinal() == 0) {
                        // find an unassigned value. Here we could try to optimize layout
                        // by assigning "bushy" values in some optimal pattern
                        // (if we knew that was...)
                        for (; assigned.contains(nextOrdinal); nextOrdinal++) {
                        }
                        tableStatus.setOrdinal(nextOrdinal);
                        userRowDef.setOrdinal(nextOrdinal);
                        assigned.add(nextOrdinal);
                    }
                }
                if (assigned.size() != groupRowDef.getUserTableRowDefs().length) {
                    throw new IllegalStateException(
                        String.format("Inconsistent ordinal number assignments: %s", assigned));
                }
            }
        }
    }

    private RowDef createUserTableRowDef(AkibaInformationSchema ais, UserTable table) {
        RowDef rowDef = new RowDef(table);
        int autoIncrementField =
            table.getAutoIncrementColumn() == null
            ? -1
            : table.getAutoIncrementColumn().getPosition();
        // parentRowDef
        int[] parentJoinFields;
        if (table.getParentJoin() != null) {
            final Join join = table.getParentJoin();
            final UserTable parentTable = join.getParent();
            //
            // parentJoinFields - TODO - not sure this is right.
            //
            parentJoinFields = new int[join.getJoinColumns().size()];
            for (int index = 0; index < join.getJoinColumns().size(); index++) {
                final JoinColumn joinColumn = join.getJoinColumns().get(index);
                parentJoinFields[index] = joinColumn.getChild().getPosition();
            }
        } else {
            parentJoinFields = new int[0];
        }

        // pkFields - The columns from this table, contributing to the hkey,
        // that don't have matching
        // columns in the parent.
        int[] pkFields = null;
        final PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            Index pkIndex = primaryKey.getIndex();
            assert pkIndex != null : primaryKey;
            if (pkFields != null) {
                throw new IllegalStateException(
                        "Can't handle two PK indexes on "
                                + table.getName().getTableName());
            }
            final List<IndexColumn> indexColumns = pkIndex.getColumns();
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

        // Secondary indexes
        List<IndexDef> indexDefList = new ArrayList<IndexDef>();
        for (Index index : table.getIndexes()) {
            List<IndexColumn> indexColumns = index.getColumns();
            if (!indexColumns.isEmpty()) {
                String treeName = groupTableName + "$$" + index.getIndexId();
                IndexDef indexDef = new IndexDef(treeName, rowDef, index);
                if (index.isPrimaryKey()) {
                    indexDefList.add(0, indexDef);
                } else {
                    indexDefList.add(indexDef);
                }
            } // else: Don't create an index for an artificial IndexDef that has no fields.
        }
        rowDef.setTreeName(groupTableName);
        rowDef.setPkFields(pkFields);
        rowDef.setParentJoinFields(parentJoinFields);
        rowDef.setIndexDefs(indexDefList.toArray(new IndexDef[indexDefList.size()]));
        rowDef.setOrdinal(0);
        rowDef.setAutoIncrementField(autoIncrementField);
        return rowDef;

    }

    private RowDef createGroupTableRowDef(AkibaInformationSchema ais, GroupTable table) {
        RowDef rowDef = new RowDef(table);
        List<Integer> userTableRowDefIds = new ArrayList<Integer>();
        for (Column column : table.getColumns()) {
            Column userColumn = column.getUserColumn();
            if (userColumn.getPosition() == 0) {
                int userRowDefId = userColumn.getTable().getTableId();
                userTableRowDefIds.add(userRowDefId);
                RowDef userRowDef = cacheMap.get(userRowDefId);
                userRowDef.setColumnOffset(column.getPosition());
            }
        }
        RowDef[] userTableRowDefs = new RowDef[userTableRowDefIds.size()];
        int i = 0;
        for (Integer userTableRowDefId : userTableRowDefIds) {
            userTableRowDefs[i++] = cacheMap.get(userTableRowDefId);
        }
        final String groupTableName = table.getName().getTableName();
        // Secondary indexes
        final List<IndexDef> indexDefList = new ArrayList<IndexDef>();
        for (Index index : table.getIndexes()) {
            List<IndexColumn> indexColumns = index.getColumns();
            if (!indexColumns.isEmpty()) {
                String treeName = groupTableName + "$$" + index.getIndexId();
                IndexDef indexDef = new IndexDef(treeName, rowDef, index);
                indexDefList.add(indexDef);
            } // else: Don't create a group table index for an artificial IndeDef that has no fields.
        }
        rowDef.setTreeName(groupTableName);
        rowDef.setPkFields(new int[0]);
        rowDef.setUserTableRowDefs(userTableRowDefs);
        rowDef.setIndexDefs(indexDefList.toArray(new IndexDef[indexDefList.size()]));
        rowDef.setOrdinal(0);
        return rowDef;
    }

    RowDef lookUpRowDef(final int rowDefId) throws RowDefNotFoundException {
        throw new RowDefNotFoundException(rowDefId);
    }

    /**
     * Adds a RowDef preemptively to the cache. This is intended primarily to
     * simplify unit tests.
     * 
     * @param rowDef
     */
    public synchronized void putRowDef(final RowDef rowDef) {
        final Integer key = rowDef.getRowDefId();
        final String name = nameOf(rowDef.getSchemaName(), rowDef.getTableName());
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

    public void analyzeAll() throws RowDefNotFoundException {
        for (final RowDef rowDef : cacheMap.values()) {
            analyze(rowDef);
        }
    }

    void analyze(final RowDef rowDef) throws RowDefNotFoundException {
        rowDef.computeRowDefType(this);
        rowDef.computeFieldAssociations(this);
    }

    RowDef rowDef(Table table)
    {
        for (RowDef rowDef : cacheMap.values()) {
            if (rowDef.table() == table) {
                return rowDef;
            }
        }
        return null;
    }

    @Override
    public boolean equals(final Object o) {
        final RowDefCache cache = (RowDefCache) o;
        return cacheMap.equals(cache.cacheMap);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
