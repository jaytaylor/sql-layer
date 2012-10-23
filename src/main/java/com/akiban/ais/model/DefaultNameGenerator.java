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

package com.akiban.ais.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.akiban.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultNameGenerator implements NameGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultNameGenerator.class);

    // Use 1 as default offset because the AAM uses tableID 0 as a marker value.
    static final int USER_TABLE_ID_OFFSET = 1;
    static final int IS_TABLE_ID_OFFSET = 1000000000;
    private static final String TREE_NAME_SEPARATOR = ".";

    private final Set<String> treeNames;
    private final Set<TableName> sequenceNames;
    private final SortedSet<Integer> isTableIDSet;
    private final SortedSet<Integer> userTableIDSet;


    public DefaultNameGenerator() {
        treeNames = new HashSet<String>();
        sequenceNames = new HashSet<TableName>();
        userTableIDSet = new TreeSet<Integer>();
        isTableIDSet = new TreeSet<Integer>();
    }

    public DefaultNameGenerator(AkibanInformationSchema ais) {
        treeNames = collectTreeNames(ais);
        sequenceNames = new HashSet<TableName>(ais.getSequences().keySet());
        isTableIDSet = collectTableIDs(ais, true);
        userTableIDSet = collectTableIDs(ais, false);
    }


    @Override
    public TableName generateIdentitySequenceName(TableName tableName) {
        TableName seqName = new TableName(tableName.getSchemaName(), "_sequence-" + tableName.hashCode());
        return makeUnique(sequenceNames, seqName);
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
    public String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> columns) {
        List<String> pkColNames = new LinkedList<String>();
        List<String> fkColNames = new LinkedList<String>();
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
        SortedSet<Integer> idSet = new TreeSet<Integer>();
        Collection<Schema> schemas;
        if(onlyISTables) {
            Schema is = ais.getSchema(TableName.INFORMATION_SCHEMA);
            schemas = (is != null) ? Collections.singleton(is) : Collections.<Schema>emptyList();
        } else {
            schemas = ais.getSchemas().values();
        }
        for(Schema schema : schemas) {
            for(UserTable table : schema.getUserTables().values()) {
                idSet.add(table.getTableId());
            }
        }
        return idSet;
    }

    public static Set<String> collectTreeNames(AkibanInformationSchema ais) {
        Set<String> treeNames = new HashSet<String>();
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
            default:
                throw new IllegalArgumentException("Unknown type: " + index.getIndexType());
        }
    }
}
