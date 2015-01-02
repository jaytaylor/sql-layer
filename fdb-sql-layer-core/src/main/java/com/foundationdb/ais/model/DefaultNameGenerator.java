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

package com.foundationdb.ais.model;

import java.util.Collection;
import java.util.Collections;
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

    public static final int MAX_IDENT = 64;
    static final String IDENTITY_SEQUENCE_FORMAT = "%s_%s_seq";

    // Use 1 as default offset because the AAM uses tableID 0 as a marker value.
    static final int USER_TABLE_ID_OFFSET = 1;
    static final int IS_TABLE_ID_OFFSET = 1000000000;

    private final Set<String> fullTextPaths;
    private final SortedSet<Integer> tableIDSet;
    private final SortedSet<Integer> isTableIDSet;
    private final Map<Integer,Integer> indexIDMap;
    private final Set<TableName> constraintNameSet;


    public DefaultNameGenerator() {
        this.fullTextPaths = new HashSet<>();
        tableIDSet = new TreeSet<>();
        isTableIDSet = new TreeSet<>();
        indexIDMap = new HashMap<>();
        constraintNameSet = new HashSet<>();
    }

    public DefaultNameGenerator(AkibanInformationSchema ais) {
        this();
        mergeAIS(ais);
    }

    protected synchronized int getMaxIndexID() {
        int max = 1;
        for(Integer id : indexIDMap.values()) {
            max = Math.max(max, id);
        }
        return max;
    }

    @Override
    public synchronized int generateTableID(TableName name) {
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
    public synchronized int generateIndexID(int rootTableID) {
        Integer current = indexIDMap.get(rootTableID);
        int newID = 1;
        if(current != null) {
            newID += current;
        }
        indexIDMap.put(rootTableID, newID);
        return newID;
    }

    @Override
    public synchronized TableName generateIdentitySequenceName(AkibanInformationSchema ais, TableName table, String column) {
        String proposed = String.format(IDENTITY_SEQUENCE_FORMAT, table.getTableName(), column);
        return findUnique(ais.getSequences().keySet(), new TableName(table.getSchemaName(), proposed));
    }

    @Override
    public synchronized String generateJoinName(TableName parentTable, TableName childTable, String[] pkColNames, String [] fkColNames) {
        List<String> pkColNamesList = new LinkedList<>();
        List<String> fkColNamesList = new LinkedList<>();
        for (String col : pkColNames) {
            pkColNamesList.add(col);
        }
        for (String col : fkColNames) {
            fkColNamesList.add(col);
        }
        return generateJoinName(parentTable, childTable, pkColNamesList, fkColNamesList);
    }
    
    @Override
    public synchronized String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> columns) {
        List<String> pkColNames = new LinkedList<>();
        List<String> fkColNames = new LinkedList<>();
        for (JoinColumn col : columns) {
            pkColNames.add(col.getParent().getName());
            fkColNames.add(col.getChild().getName());
        }
        return generateJoinName(parentTable, childTable, pkColNames, fkColNames);
    }

    @Override
    public synchronized String generateJoinName(TableName parentTable, TableName childTable, List<String> pkColNames, List<String> fkColNames) {
        String ret = String.format("%s/%s/%s/%s/%s/%s",
                parentTable.getSchemaName(),
                parentTable.getTableName(),
                Strings.join(pkColNames, ","),
                childTable.getSchemaName(),
                childTable, // TODO: This should be getTableName(), but preserve old behavior for test existing output
                Strings.join(fkColNames, ","));
        String generatedName = ret.replace(',', '_');
        TableName constrName = findUnique(constraintNameSet, new TableName(parentTable.getSchemaName(), generatedName));
        Boolean newConstraintName = constraintNameSet.add(constrName);
        assert(newConstraintName);
        return constrName.getTableName();
    }

    @Override
    public synchronized String generateFullTextIndexPath(FullTextIndex index) {
        IndexName name = index.getIndexName();
        String proposed = String.format("%s.%s.%s", name.getSchemaName(), name.getTableName(), name.getName());
        return makeUnique(fullTextPaths, proposed, MAX_IDENT);
    }

    @Override
    public synchronized TableName generateFKConstraintName(String schemaName, String tableName) {
        return generateConstraintName(schemaName, tableName, "fkey");
    }

    @Override
    public synchronized TableName generatePKConstraintName( String schemaName, String tableName) {
        return generateConstraintName(schemaName, tableName, "pkey");
    }

    @Override
    public synchronized TableName generateUniqueConstraintName( String schemaName, String tableName) {
        return generateConstraintName(schemaName, tableName, "ukey");
    }

    private TableName generateConstraintName(String schemaName, String tableName, String postfix) {
        String proposed = String.format("%s_%s", tableName, postfix);
        TableName constrName = findUnique(constraintNameSet, new TableName(schemaName, proposed));
        Boolean newConstraintName = constraintNameSet.add(constrName);
        assert(newConstraintName);
        return constrName;
    }
    
    @Override
    public synchronized void mergeAIS(AkibanInformationSchema ais) {
        isTableIDSet.addAll(collectTableIDs(ais, true));
        tableIDSet.addAll(collectTableIDs(ais, false));
        indexIDMap.putAll(collectMaxIndexIDs(ais));
        constraintNameSet.addAll(ais.getConstraints().keySet());
    }

    @Override
    public synchronized void removeTableID(int tableID) {
        isTableIDSet.remove(tableID);
        tableIDSet.remove(tableID);
    }

    /** Should be over-ridden by derived. */
    @Override
    public synchronized Set<String> getStorageNames() {
        return Collections.emptySet();
    }

    //
    // Private
    //

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
            nextID = tableIDSet.isEmpty() ? USER_TABLE_ID_OFFSET : tableIDSet.last() + 1;
        }
        while(isTableIDSet.contains(nextID) || tableIDSet.contains(nextID)) {
            nextID += 1;
        }
        if(isISTable) {
            isTableIDSet.add(nextID);
        } else {
            tableIDSet.add(nextID);
        }
        return nextID;
    }

    //
    // Static
    //

    private static SortedSet<Integer> collectTableIDs(AkibanInformationSchema ais, boolean onlyISTables) {
        SortedSet<Integer> idSet = new TreeSet<>();
        for(Schema schema : ais.getSchemas().values()) {
            if(TableName.INFORMATION_SCHEMA.equals(schema.getName()) != onlyISTables) {
                continue;
            }
            for(Table table : schema.getTables().values()) {
                idSet.add(table.getTableId());
            }
        }
        return idSet;
    }

    public static Map<Integer,Integer> collectMaxIndexIDs(AkibanInformationSchema ais) {
        MaxIndexIDVisitor visitor = new MaxIndexIDVisitor();
        Map<Integer,Integer> idMap = new HashMap<>();
        for(Group group : ais.getGroups().values()) {
            visitor.reset();
            group.visit(visitor);
            idMap.put(group.getRoot().getTableId(), visitor.getMaxIndexID());
        }
        return idMap;
    }

    /** Find a name that would be unique if added to {@code set}. */
    private static TableName findUnique(Collection<TableName> set, TableName original) {
        int counter = 1;
        String baseName = original.getTableName();
        TableName proposed = original;
        while(set.contains(proposed) || (proposed.getTableName().length() > MAX_IDENT)) {
            String countStr = "$" + counter++;
            int diff = baseName.length() + countStr.length() - MAX_IDENT;
            if(diff > 0) {
                baseName = truncate(baseName, baseName.length() - diff);
            }
            proposed = new TableName(original.getSchemaName(), baseName + countStr);
        }
        return proposed;
    }

    public static String findUnique(Collection<String> set, String original, int maxLength) {
        int counter = 1;
        String baseName = original;
        String proposed = original;
        while(set.contains(proposed) || (proposed.length() > maxLength)) {
            String countStr = "$" + counter++;
            int diff = baseName.length() + countStr.length() - maxLength;
            if(diff > 0) {
                baseName = truncate(baseName, baseName.length() - diff);
            }
            proposed = baseName + countStr;
        }
        return proposed;
    }

    public static String makeUnique(Collection<String> treeNames, String proposed, int maxLength) {
        String actual = findUnique(treeNames, proposed, maxLength);
        treeNames.add(actual);
        return actual;
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

    private static String truncate(String s, int maxLen) {
        return (s.length() > maxLen) ? s.substring(0, maxLen) : s;
    }

    private static class MaxIndexIDVisitor extends AbstractVisitor {
        private int maxID;

        public MaxIndexIDVisitor() {
        }

        public void reset() {
            maxID = 0;
        }

        public int getMaxIndexID() {
            return maxID;
        }

        @Override
        public void visit(Index index) {
            maxID = Math.max(maxID, index.getIndexId());
        }
    }
}
