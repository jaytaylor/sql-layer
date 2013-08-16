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

package com.foundationdb.ais.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.foundationdb.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultNameGenerator implements NameGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultNameGenerator.class);

    public static final String IDENTITY_SEQUENCE_PREFIX = "_sequence-";

    // Use 1 as default offset because the AAM uses tableID 0 as a marker value.
    static final int USER_TABLE_ID_OFFSET = 1;
    static final int IS_TABLE_ID_OFFSET = 1000000000;
    private static final String TREE_NAME_SEPARATOR = ".";

    private final Set<String> treeNames;
    private final Set<TableName> sequenceNames;
    private final SortedSet<Integer> isTableIDSet;
    private final SortedSet<Integer> userTableIDSet;
    private final Map<Integer,Integer> indexIDMap;


    public DefaultNameGenerator() {
        treeNames = new HashSet<>();
        sequenceNames = new HashSet<>();
        userTableIDSet = new TreeSet<>();
        isTableIDSet = new TreeSet<>();
        indexIDMap = new HashMap<>();
    }

    public DefaultNameGenerator(AkibanInformationSchema ais) {
        this();
        mergeAIS(ais);
    }

    int getMaxIndexID() {
        int max = 1;
        for(Integer id : indexIDMap.values()) {
            max = Math.max(max, id);
        }
        return max;
    }


    @Override
    public int generateTableID(TableName name) {
        final int offset;
        if(TableName.INFORMATION_SCHEMA.equals(name.getSchemaName())) {
            offset = getNextTableID(true);
            assert offset >= IS_TABLE_ID_OFFSET : "Offset too small for IS table " + name + ": " + offset;
        } else {
            offset = getNextTableID(false);
            if(offset >= IS_TABLE_ID_OFFSET) {
                LOG.warn("Offset for table {} unexpectedly large: {}", name, offset);
            }
        }
        return offset;
    }

    @Override
    public int generateIndexID(int rootTableID) {
        Integer current = indexIDMap.get(rootTableID);
        int newID = 1;
        if(current != null) {
            newID += current;
        }
        indexIDMap.put(rootTableID, newID);
        return newID;
    }

    @Override
    public TableName generateIdentitySequenceName(TableName tableName) {
        TableName seqName = new TableName(tableName.getSchemaName(), IDENTITY_SEQUENCE_PREFIX + tableName.hashCode());
        return makeUnique(sequenceNames, seqName);
    }

    @Override
    public String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> columns) {
        List<String> pkColNames = new LinkedList<>();
        List<String> fkColNames = new LinkedList<>();
        for (JoinColumn col : columns) {
            pkColNames.add(col.getParent().getName());
            fkColNames.add(col.getChild().getName());
        }
        return generateJoinName(parentTable, childTable, pkColNames, fkColNames);
    }

    @Override
    public String generateJoinName(TableName parentTable, TableName childTable, List<String> pkColNames, List<String> fkColNames) {
        String ret = String.format("%s/%s/%s/%s/%s/%s",
                parentTable.getSchemaName(),
                parentTable.getTableName(),
                Strings.join(pkColNames, ","),
                childTable.getSchemaName(),
                childTable, // TODO: This shold be getTableName(), but preserve old behavior for test existing output
                Strings.join(fkColNames, ","));
        return ret.toLowerCase().replace(',', '_');
    }

    @Override
    public String generateIndexTreeName(Index index) {
        // schema.table.index
        final TableName tableName;
        switch(index.getIndexType()) {
            case TABLE:
                tableName = ((TableIndex)index).getTable().getName();
            break;
            case GROUP:
                UserTable root = ((GroupIndex)index).getGroup().getRoot();
                if(root == null) {
                    throw new IllegalArgumentException("Grouping incomplete (no root)");
                }
                tableName = root.getName();
            break;
            case FULL_TEXT:
                tableName = ((FullTextIndex)index).getIndexedTable().getName();
            break;
            default:
                throw new IllegalArgumentException("Unknown type: " + index.getIndexType());
        }
        String proposed = escapeForTreeName(tableName.getSchemaName()) + TREE_NAME_SEPARATOR +
                          escapeForTreeName(tableName.getTableName()) + TREE_NAME_SEPARATOR +
                          escapeForTreeName(index.getIndexName().getName());
        return makeUnique(treeNames, proposed);
    }

    @Override
    public String generateGroupTreeName(String schemaName, String groupName) {
        // schema.group_name
        String proposed = escapeForTreeName(schemaName) + TREE_NAME_SEPARATOR +
                          escapeForTreeName(groupName);
        return makeUnique(treeNames, proposed);
    }

    @Override
    public String generateSequenceTreeName(Sequence sequence) {
        TableName tableName = sequence.getSequenceName();
        String proposed = escapeForTreeName(tableName.getSchemaName()) + TREE_NAME_SEPARATOR +
                          escapeForTreeName(tableName.getTableName());
        return makeUnique(treeNames, proposed);
    }

    @Override
    public void mergeAIS(AkibanInformationSchema ais) {
        treeNames.addAll(collectTreeNames(ais));
        sequenceNames.addAll(ais.getSequences().keySet());
        isTableIDSet.addAll(collectTableIDs(ais, true));
        userTableIDSet.addAll(collectTableIDs(ais, false));
        indexIDMap.putAll(collectMaxIndexIDs(ais));
    }

    @Override
    public void removeTableID(int tableID) {
        isTableIDSet.remove(tableID);
        userTableIDSet.remove(tableID);
    }

    @Override
    public void removeTreeName(String treeName) {
        treeNames.remove(treeName);
    }

    @Override
    public Set<String> getTreeNames() {
        return new TreeSet<>(treeNames);
    }


    /**
     * Get the next number that could be used for a table ID. The parameter indicates
     * where to start the search, but the ID will be unique across ALL tables.
     * @param isISTable Offset to start the search at.
     * @return Unique ID value.
     */
    private int getNextTableID(boolean isISTable) {
        int nextID;
        if(isISTable) {
            nextID = isTableIDSet.isEmpty() ? IS_TABLE_ID_OFFSET : isTableIDSet.last() + 1;
        } else {
            nextID = userTableIDSet.isEmpty() ? USER_TABLE_ID_OFFSET : userTableIDSet.last() + 1;
        }
        while(isTableIDSet.contains(nextID) || userTableIDSet.contains(nextID)) {
            nextID += 1;
        }
        if(isISTable) {
            isTableIDSet.add(nextID);
        } else {
            userTableIDSet.add(nextID);
        }
        return nextID;
    }

    private static SortedSet<Integer> collectTableIDs(AkibanInformationSchema ais, boolean onlyISTables) {
        SortedSet<Integer> idSet = new TreeSet<>();
        for(Schema schema : ais.getSchemas().values()) {
            if(TableName.INFORMATION_SCHEMA.equals(schema.getName()) != onlyISTables) {
                continue;
            }
            for(UserTable table : schema.getUserTables().values()) {
                idSet.add(table.getTableId());
            }
        }
        return idSet;
    }

    public static Map<Integer,Integer> collectMaxIndexIDs(AkibanInformationSchema ais) {
        MaxIndexIDVisitor visitor = new MaxIndexIDVisitor();
        Map<Integer,Integer> idMap = new HashMap<>();
        for(Group group : ais.getGroups().values()) {
            visitor.resetAndVisit(group);
            idMap.put(group.getRoot().getTableId(), visitor.getMaxIndexID());
        }
        return idMap;
    }

    public static Set<String> collectTreeNames(AkibanInformationSchema ais) {
        Set<String> treeNames = new HashSet<>();
        for(Group group : ais.getGroups().values()) {
            treeNames.add(group.getTreeName());
            for(Index index : group.getIndexes()) {
                treeNames.add(index.getTreeName());
            }
        }
        for(UserTable table : ais.getUserTables().values()) {
            for(Index index : table.getIndexesIncludingInternal()) {
                treeNames.add(index.getTreeName());
            }
        }
        for (Sequence sequence : ais.getSequences().values()){
            if(sequence.getTreeName() != null) {
                treeNames.add(sequence.getTreeName());
            }
        }
        return treeNames;
    }

    private static TableName makeUnique(Set<TableName> set, TableName original) {
        int counter = 1;
        TableName proposed = original;
        while(!set.add(proposed)) {
            proposed = new TableName(original.getSchemaName(), original.getTableName()  + "$" + counter++);
        }
        return proposed;
    }

    private static String makeUnique(Set<String> set, String original) {
        int counter = 1;
        String proposed = original;
        while(!set.add(proposed)) {
            proposed = original + "$" + counter++;
        }
        return proposed;
    }

    public static String escapeForTreeName(String name) {
        return name.replace(TREE_NAME_SEPARATOR, "\\" + TREE_NAME_SEPARATOR);
    }

    public static String schemaNameForIndex(Index index) {
        switch(index.getIndexType()) {
            case TABLE:
                return ((TableIndex)index).getTable().getName().getSchemaName();
            case GROUP:
                return ((GroupIndex)index).getGroup().getSchemaName();
            case FULL_TEXT:
                return ((FullTextIndex)index).getIndexedTable().getName().getSchemaName();
            default:
                throw new IllegalArgumentException("Unknown type: " + index.getIndexType());
        }
    }

    private static class MaxIndexIDVisitor extends NopVisitor {
        private int maxID;

        public MaxIndexIDVisitor() {
        }

        public void resetAndVisit(Group group) {
            maxID = 0;
            visitGroup(group);
            group.getRoot().traverseTableAndDescendants(this);
        }

        public int getMaxIndexID() {
            return maxID;
        }

        @Override
        public void visitGroup(Group group) {
            checkIndexes(group.getIndexes());
        }

        @Override
        public void visitUserTable(UserTable table) {
            checkIndexes(table.getIndexesIncludingInternal());
        }

        private void checkIndexes(Collection<? extends Index> indexes) {
            for(Index index : indexes) {
                maxID = Math.max(maxID, index.getIndexId());
            }
        }
    }
}
