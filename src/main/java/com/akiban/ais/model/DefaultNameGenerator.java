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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.akiban.ais.model.AISBuilder.ColumnName;
import com.akiban.util.Strings;

public class DefaultNameGenerator implements NameGenerator {
    public static final String TREE_NAME_SEPARATOR = ".";

    /**
     * For truncated columns [only], we record a mapping of the original
     * column to truncated name. This lets us ensure that unique columns
     * have unique truncated names. We use HashMap instead of Map to make
     * life easier for GWT.
     */
    private final HashMap<ColumnName, String> generatedColumnNames = new HashMap<ColumnName, String>();
    private final Set<String> groupNames = new HashSet<String>();
    private final Set<String> indexNames = new HashSet<String>();
    private final Set<String> treeNames = new HashSet<String>();
    private final Set<String> sequenceNames = new HashSet<String>();
    
    @Override
    public String generateColumnName(Column column) {
        UserTable table = (UserTable) column.getTable();

        // Return existing if we've already generated one for this column
        final ColumnName id = new ColumnName(table.getName(), column.getName());
        {
            String possible = generatedColumnNames.get(id);
            if (possible != null) {
                return possible;
            }
        }

        StringBuilder ret = new StringBuilder(table.getName().getTableName()).append("$").append(column.getName());
        if (ret.length() > AISBuilder.MAX_COLUMN_NAME_LENGTH) {
            ret.delete(0, ret.length() - AISBuilder.MAX_COLUMN_NAME_LENGTH);
        }

        int anonId = 0;
        int keepLen = ret.length();
        String retValue;
        while (generatedColumnNames.containsValue(retValue = ret.toString())) {
            ret.setLength(keepLen);
            int digits = countDigits(++anonId);
            int newLenOverflow = AISBuilder.MAX_COLUMN_NAME_LENGTH - (keepLen + digits + 1);
            if (newLenOverflow < 0) {
                keepLen += newLenOverflow;
                ret.setLength(keepLen);
            }
            ret.append('$').append(anonId);
        }

        generatedColumnNames.put(id, retValue);

        return retValue;
    }
    
    /**
     * Counts the number of digits in the int
     * 
     * @param number
     *            {@code >= 0}
     * @return number of digits
     */
    private int countDigits(int number) {
        int ret = 1;
        while ((number /= 10) > 0) {
            ++ret;
        }
        return ret;
    }

    @Override
    public String generateGroupTableIndexName(TableIndex userTableIndex) {
        return userTableIndex.getTable().getName().getTableName() + "$"
        + userTableIndex.getIndexName().getName();
    }

    @Override
    public String generateGroupName(UserTable userTable) {
        return generateGroupName(userTable.getName().getTableName());
    }
    
    @Override
    public String generateGroupName(final String tableName) {
        String proposed = tableName;
        return makeUnique(groupNames, proposed);
    }

    @Override
    public String generateGroupTableName (final String groupName) {
        return "_akiban_" + groupName;
    }

    public DefaultNameGenerator setDefaultGroupNames (Set<String> initialSet) {
        groupNames.addAll(initialSet);
        return this;
    }

    public DefaultNameGenerator setDefaultTreeNames (Set<String> initialSet) {
        treeNames.addAll(initialSet);
        return this;
    }

    public DefaultNameGenerator setDefaultSequenceNames (Set<String> initialSet) {
        sequenceNames.addAll(initialSet);
        return this;
    }
    
    @Override
    public String generateIndexName(String indexName, String columnName,
            String constraint) {
        if (constraint.equals(Index.PRIMARY_KEY_CONSTRAINT)) {
            indexNames.add(Index.PRIMARY_KEY_CONSTRAINT);
            return Index.PRIMARY_KEY_CONSTRAINT;
        }
        
        if (indexName != null && !indexNames.contains(indexName)) {
            indexNames.add(indexName);
            return indexName;
        }
        
        String name = columnName;
        for (int suffixNum=2; indexNames.contains(name); ++suffixNum) {
            name = String.format("%s_%d", columnName, suffixNum);
        }
        indexNames.add(name);
        return name;
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
                UserTable root = ((GroupIndex)index).getGroup().getGroupTable().getRoot();
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
    public String generateGroupTreeName(Group group) {
        // schema.group_name
        TableName tableName = group.getGroupTable().getName();
        String proposed = escapeForTreeName(tableName.getSchemaName()) + TREE_NAME_SEPARATOR +
                          escapeForTreeName(group.getName());
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
        final Table table;
        switch(index.getIndexType()) {
            case TABLE:
                table = ((TableIndex)index).getTable();
                break;
            case GROUP:
                table = ((GroupIndex)index).getGroup().getGroupTable();
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + index.getIndexType());
        }
        return table.getName().getSchemaName();
    }
}
