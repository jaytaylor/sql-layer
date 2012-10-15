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

package com.akiban.server.rowdata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.TableStatus;
import com.akiban.server.TableStatusCache;
import com.persistit.exception.PersistitInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.RowDefNotFoundException;

/**
 * Caches RowDef instances. In this incarnation, this class also constructs
 * RowDef objects from the AkibanInformationSchema. The translation is done in
 * the {@link #setAIS(AkibanInformationSchema)} method.
 * 
 * @author peter
 */
public class RowDefCache {
    private static final Logger LOG = LoggerFactory.getLogger(RowDefCache.class.getName());

    private static volatile RowDefCache LATEST;

    private final Map<Integer, RowDef> cacheMap = new TreeMap<Integer, RowDef>();
    private final Map<TableName, Integer> nameMap = new TreeMap<TableName, Integer>();
    protected final TableStatusCache tableStatusCache;
    private AkibanInformationSchema ais;

    public RowDefCache(final TableStatusCache tableStatusCache) {
        this.tableStatusCache = tableStatusCache;
        LATEST = this;
    }

    /** Should <b>only</b> be used for debugging (e.g. friendly toString). This view is not transaction safe. **/
    public static RowDefCache latest() {
        return LATEST;
    }

    /**
     * Look up and return a RowDef for a supplied rowDefId value.
     * @param rowDefId ID to lookup.
     * @return The corresponding RowDef
     * @throws RowDefNotFoundException if there is no such RowDef.
     */
    public synchronized RowDef getRowDef(final int rowDefId) throws RowDefNotFoundException {
        RowDef rowDef = rowDef(rowDefId);
        if (rowDef == null) {
            throw new RowDefNotFoundException(rowDefId);
        }
        return rowDef;
    }

    /**
     * Look up and return a RowDef for a supplied rowDefId value or null if none exists.
     * @param rowDefId ID to lookup.
     * @return The corresponding RowDef object or <code>null</code>
     */
    public synchronized RowDef rowDef(final int rowDefId) {
        return cacheMap.get(rowDefId);
    }

    public synchronized List<RowDef> getRowDefs() {
        return new ArrayList<RowDef>(cacheMap.values());
    }

    public synchronized RowDef getRowDef(TableName tableName) throws RowDefNotFoundException {
        final Integer key = nameMap.get(tableName);
        if (key == null) {
            return null;
        }
        return getRowDef(key);
    }

    public RowDef getRowDef(String schema, String table) throws RowDefNotFoundException {
        return getRowDef(new TableName(schema, table));
    }

    public synchronized void clear() {
        cacheMap.clear();
        nameMap.clear();
    }

    /**
     * Receive an instance of the AkibanInformationSchema, crack it and produce
     * the RowDef instances it defines.
     */
    public synchronized void setAIS(final AkibanInformationSchema ais) throws PersistitInterruptedException {
        this.ais = ais;
        
        for (final UserTable table : ais.getUserTables().values()) {
            putRowDef(createUserTableRowDef(table));
        }

        analyzeAll();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug(toString());
        }
    }

    public AkibanInformationSchema ais()
    {
        return ais;
    }

    /**
     * Assign "ordinal" values to user table RowDef instances. An ordinal the
     * integer used to identify a user table subtree within an hkey. This method
     * Assigned unique integers where needed to any tables that have not already
     * received non-zero ordinal values. Once a table is populated, its ordinal
     * is written as part of the TableStatus record, and on subsequent server
     * start-ups, that value is loaded and reused from the status tree.
     * @return Map of Table->Ordinal for all Tables/RowDefs in the RowDefCache
     */
    protected Map<Table,Integer> fixUpOrdinals() throws PersistitInterruptedException {
        Map<Group,List<RowDef>> groupToRowDefs = getRowDefsByGroup();
        Map<Table,Integer> ordinalMap = new HashMap<Table,Integer>();
        for(List<RowDef> rowDefs  : groupToRowDefs.values()) {
            // First pass: merge already assigned values
            HashSet<Integer> assigned = new HashSet<Integer>();
            for(RowDef rowDef : rowDefs) {
                int ordinal = rowDef.getTableStatus().getOrdinal();
                if(ordinal != 0 && !assigned.add(ordinal)) {
                    throw new IllegalStateException("Non-unique ordinal value " + ordinal + " added to " + assigned);
                }
            }

            // Second pass: assign new ordinals
            int nextOrdinal = 1;
            for(RowDef rowDef : rowDefs) {
                int ordinal = rowDef.getTableStatus().getOrdinal();
                if (ordinal == 0) {
                    while(assigned.contains(nextOrdinal)) {
                        ++nextOrdinal;
                    }
                    ordinal = nextOrdinal++;
                    tableStatusCache.setOrdinal(rowDef.getRowDefId(), ordinal);
                }
                assigned.add(ordinal);
                ordinalMap.put(rowDef.table(), ordinal);
                rowDef.setOrdinalCache(ordinal);
            }

            if(assigned.size() != rowDefs.size()) {
                throw new IllegalStateException("Inconsistent ordinal number assignments: " + assigned);
            }
        }
        return ordinalMap;
    }

    private RowDef createRowDefCommon(Table table, MemoryTableFactory factory) {
        final TableStatus status;
        if(factory == null) {
            status = tableStatusCache.getTableStatus(table.getTableId());
        } else {
            status = tableStatusCache.getMemoryTableStatus(table.getTableId(), factory);
        }
        return new RowDef(table, status);
    }

    private RowDef createUserTableRowDef(UserTable table) throws PersistitInterruptedException {
        RowDef rowDef = createRowDefCommon(table, table.getMemoryTableFactory());
        // parentRowDef
        int[] parentJoinFields;
        if (table.getParentJoin() != null) {
            final Join join = table.getParentJoin();
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

        // root table
        UserTable root = table;
        while (root.getParentJoin() != null) {
            root = root.getParentJoin().getParent();
        }

        // Secondary indexes
        List<TableIndex> indexList = new ArrayList<TableIndex>();
        for (TableIndex index : table.getIndexesIncludingInternal()) {
            List<IndexColumn> indexColumns = index.getKeyColumns();
            if(!indexColumns.isEmpty()) {
                new IndexDef(rowDef, index);
                if (index.isPrimaryKey()) {
                    indexList.add(0, index);
                } else {
                    indexList.add(index);
                }
            }
            //else Don't create IndexDef for empty, autogenerated indexes
        }

        // Group indexes
        final List<GroupIndex> groupIndexList = new ArrayList<GroupIndex>();
        for (GroupIndex index : table.getGroupIndexes()) {
            if(index.leafMostTable() == table) {
                new IndexDef(rowDef, index); // Hooks itself to the index
                groupIndexList.add(index);
            }
        }

        rowDef.setParentJoinFields(parentJoinFields);
        rowDef.setIndexes(indexList.toArray(new TableIndex[indexList.size()]));
        rowDef.setGroupIndexes(groupIndexList.toArray(new GroupIndex[groupIndexList.size()]));

        tableStatusCache.setRowDef(rowDef.getRowDefId(), rowDef);
        Column autoIncColumn = table.getAutoIncrementColumn();
        if(autoIncColumn != null) {
            long initialAutoIncrementValue = autoIncColumn.getInitialAutoIncrementValue();
            tableStatusCache.setAutoIncrement(table.getTableId(), initialAutoIncrementValue);
        }

        return rowDef;
    }
    
    private synchronized void putRowDef(final RowDef rowDef) {
        final Integer key = rowDef.getRowDefId();
        final TableName name = new TableName(rowDef.getSchemaName(), rowDef.getTableName());
        if (cacheMap.containsKey(key)) {
            throw new IllegalStateException("Duplicate RowDefID (" + key + ") for RowDef: " + rowDef);
        }
        if (nameMap.containsKey(name)) {
            throw new IllegalStateException("Duplicate name (" + name + ") for RowDef: " + rowDef);
        }
        cacheMap.put(key, rowDef);
        nameMap.put(name, key);
    }
    
    private void analyzeAll() throws PersistitInterruptedException {
        Map<Table,Integer> ordinalMap = fixUpOrdinals();
        for (final RowDef rowDef : cacheMap.values()) {
            rowDef.computeFieldAssociations(ordinalMap);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("\n");
        for (Map.Entry<TableName, Integer> entry : nameMap.entrySet()) {
            final RowDef rowDef = cacheMap.get(entry.getValue());
            sb.append("   ");
            sb.append(rowDef);
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        if(!(o instanceof RowDefCache)) {
            return false;
        }
        RowDefCache that = (RowDefCache) o;
        if(cacheMap == null) {
            return that.cacheMap == null;
        }
        return cacheMap.equals(that.cacheMap);
    }

    @Override
    public int hashCode() {
        return cacheMap.hashCode();
    }

    protected Map<Group,List<RowDef>> getRowDefsByGroup() {
        Map<Group,List<RowDef>> groupToRowDefs = new HashMap<Group, List<RowDef>>();
        for(RowDef rowDef : getRowDefs()) {
            List<RowDef> list = groupToRowDefs.get(rowDef.getGroup());
            if(list == null) {
                list = new ArrayList<RowDef>();
                groupToRowDefs.put(rowDef.getGroup(), list);
            }
            list.add(rowDef);
        }
        // NB: Ordinals should be increasing from root to leaf. Sort ensures that.
        for(List<RowDef> rowDefs : groupToRowDefs.values()) {
            Collections.sort(rowDefs, ROWDEF_DEPTH_COMPARATOR);
        }
        return groupToRowDefs;
    }

    /** By group depth and then qualified table name for determinism **/
    static Comparator<RowDef> ROWDEF_DEPTH_COMPARATOR = new Comparator<RowDef>() {
        @Override
        public int compare(RowDef o1, RowDef o2) {
            int cmp = o1.userTable().getDepth().compareTo(o2.userTable().getDepth());
            if(cmp == 0) {
                cmp = o1.table().getName().compareTo(o2.table().getName());
            }
            return cmp;
        }
    };
}
