/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
    public static RowDefCache latestForDebugging() {
        return LATEST;
    }

    /**
     * Receive an instance of the AkibanInformationSchema, crack it and produce
     * the RowDef instances it defines.
     */
    public synchronized void setAIS(final AkibanInformationSchema newAIS) throws PersistitInterruptedException {
        ais = newAIS;

        Map<Integer, RowDef> newRowDefs = new TreeMap<>();
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
     * Assign "ordinal" values to tables. An ordinal is an the integer
     * used to identify a user table subtree within an hkey. This method
     * assigns unique integers where needed to any tables that have not already
     * received non-null ordinal values. Once a table is populated, its ordinal
     * is written as part of the AIS.
     *
     * @return Map of Table->Ordinal for all Tables/RowDefs in the RowDefCache
     */
    protected Map<Table,Integer> fixUpOrdinals() throws PersistitInterruptedException {
        Map<Group,List<RowDef>> groupToRowDefs = getRowDefsByGroup();
        Map<Table,Integer> ordinalMap = new HashMap<>();
        for(List<RowDef> rowDefs  : groupToRowDefs.values()) {
            // First pass: merge already assigned values
            HashSet<Integer> assigned = new HashSet<>();
            for(RowDef rowDef : rowDefs) {
                Integer ordinal = rowDef.userTable().getOrdinal();
                if(ordinal != null && !assigned.add(ordinal)) {
                    throw new IllegalStateException("Non-unique ordinal value " + ordinal + " added to " + assigned);
                }
            }

            // Second pass: assign new ordinals
            int nextOrdinal = 1;
            for(RowDef rowDef : rowDefs) {
                Integer ordinal = rowDef.userTable().getOrdinal();
                if (ordinal == null) {
                    while(assigned.contains(nextOrdinal)) {
                        ++nextOrdinal;
                    }
                    ordinal = nextOrdinal++;
                    rowDef.userTable().setOrdinal(ordinal);
                }
                assigned.add(ordinal);
                ordinalMap.put(rowDef.table(), ordinal);
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
            status = tableStatusCache.createTableStatus(table.getTableId());
        } else {
            status = tableStatusCache.getOrCreateMemoryTableStatus(table.getTableId(), factory);
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
        List<TableIndex> indexList = new ArrayList<>();
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
        final List<GroupIndex> groupIndexList = new ArrayList<>();
        for (GroupIndex index : table.getGroupIndexes()) {
            if(index.leafMostTable() == table) {
                new IndexDef(rowDef, index); // Hooks itself to the index
                groupIndexList.add(index);
            }
        }

        rowDef.setParentJoinFields(parentJoinFields);
        rowDef.setIndexes(indexList.toArray(new TableIndex[indexList.size()]));
        rowDef.setGroupIndexes(groupIndexList.toArray(new GroupIndex[groupIndexList.size()]));

        rowDef.getTableStatus().setRowDef(rowDef);
        Column autoIncColumn = table.getAutoIncrementColumn();
        if(autoIncColumn != null) {
            long initialAutoIncrementValue = autoIncColumn.getInitialAutoIncrementValue();
            rowDef.getTableStatus().setAutoIncrement(initialAutoIncrementValue);
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
        Map<Group,List<RowDef>> groupToRowDefs = new HashMap<>();
        for(Table table : ais.getUserTables().values()) {
            RowDef rowDef = table.rowDef();
            List<RowDef> list = groupToRowDefs.get(rowDef.getGroup());
            if(list == null) {
                list = new ArrayList<>();
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
