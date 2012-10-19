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
     * Receive an instance of the AkibanInformationSchema, crack it and produce
     * the RowDef instances it defines.
     */
    public synchronized void setAIS(final AkibanInformationSchema newAIS) throws PersistitInterruptedException {
        ais = newAIS;

        Map<Integer, RowDef> newRowDefs = new TreeMap<Integer,RowDef>();
        for (final UserTable table : ais.getUserTables().values()) {
            RowDef rowDef = createUserTableRowDef(table);
            Integer key = rowDef.getRowDefId();
            RowDef prev = newRowDefs.put(key, rowDef);
            if (prev != null) {
                throw new IllegalStateException("Duplicate RowDefID (" + key + ") for RowDef: " + rowDef);
            }
        }

        Map<Table,Integer> ordinalMap = fixUpOrdinals();
        for (RowDef rowDef : newRowDefs.values()) {
            rowDef.computeFieldAssociations(ordinalMap);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(toString());
        }
    }

    public synchronized AkibanInformationSchema ais() {
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
        return new RowDef(table, status); // Hooks up table's rowDef too
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (UserTable table : ais().getUserTables().values()) {
            if(sb.length() > 0) {
                sb.append("\n");
            }
            sb.append("   ");
            sb.append(table.rowDef().toString());
        }
        return sb.toString();
    }

    protected synchronized Map<Group,List<RowDef>> getRowDefsByGroup() {
        Map<Group,List<RowDef>> groupToRowDefs = new HashMap<Group, List<RowDef>>();
        for(Table table : ais.getUserTables().values()) {
            RowDef rowDef = table.rowDef();
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
