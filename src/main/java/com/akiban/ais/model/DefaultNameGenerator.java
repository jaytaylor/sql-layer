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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.akiban.util.Strings;

public class DefaultNameGenerator implements NameGenerator {
    public static final String TREE_NAME_SEPARATOR = ".";

    private final Set<String> treeNames = new HashSet<String>();
    private final Set<String> sequenceNames = new HashSet<String>();

    public DefaultNameGenerator setDefaultTreeNames (Set<String> initialSet) {
        treeNames.addAll(initialSet);
        return this;
    }

    public DefaultNameGenerator setDefaultSequenceNames (Set<String> initialSet) {
        sequenceNames.addAll(initialSet);
        return this;
    }
    
    @Override
    public String generateJoinName (TableName parentTable, TableName childTable, List<JoinColumn> columns) {
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
    public String generateIdentitySequenceTreeName (Sequence sequence) {
        TableName tableName = sequence.getSequenceName();
        String proposed = escapeForTreeName(tableName.getSchemaName()) + TREE_NAME_SEPARATOR +
                          escapeForTreeName(tableName.getTableName());
        return makeUnique(treeNames, proposed);
    }
    
    @Override
    public String generateIdentitySequenceName (TableName tableName) {
        return makeUnique(sequenceNames, "_sequence-" + tableName.hashCode());
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
