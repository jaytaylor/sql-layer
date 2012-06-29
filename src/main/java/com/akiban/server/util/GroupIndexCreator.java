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

package com.akiban.server.util;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.NameIsNullException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchGroupException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.WrongNameFormatException;
/**
 * Builds an AIS Fragment of a GroupIndex to be merged into the full AIS by DDLFunctions
 * 
 *
 * @author tjoneslo
 */
public class GroupIndexCreator {
    public static class GroupIndexCreatorException extends Exception {
        public GroupIndexCreatorException(String msg) {
            super(msg);
        }
    }

    /**
     * Helper function for converting a simple group index specification into an
     * actual, non-unique GroupIndex. This can then be passed to DDLFunctions.createGroupIndex().
     *
     * TODO: This should be using AISInvariants for internal checking. 
     * 
     * @param ais AIS that contains referenced group and tables.
     * @param groupName Name of the group to add index too
     * @param indexName Name of the new index to create
     * @param tableColumnList Comma separated list of tableName.columnName pairs.
     * @return GroupIndex representation of the requested
     * @throws GroupIndexCreatorException For any error
     */
    public static GroupIndex createIndex(AkibanInformationSchema ais, String groupName, String indexName,
                                         String tableColumnList, Index.JoinType joinType) {
        return createIndex(ais, groupName, indexName, false, tableColumnList, joinType);
    }
    /**
     * Helper function for converting a simple group index specification into an
     * actual GroupIndex. This can then be passed to DDLFunctions.createGroupIndex().
     *
     * @param ais AIS that contains referenced group and tables.
     * @param groupName Name of the group to add index too
     * @param indexName Name of the new index to create
     * @param unique whether the group index is UNIQUE
     * @param tableColumnList Comma separated list of tableName.columnName pairs.
     * @return GroupIndex representation of the requested
     * @throws GroupIndexCreatorException For any error
     */
    private static GroupIndex createIndex(AkibanInformationSchema ais, String groupName, String indexName,
                                         boolean unique, String tableColumnList, Index.JoinType joinType) {
        final Group group = ais.getGroup(groupName);
        if(group == null) {
            throw new NoSuchGroupException (groupName);
        }
        if(indexName.isEmpty()) {
            throw new NameIsNullException ("group index", "index name");
        }

        final String tableColPairs[] = tableColumnList.split(",");

        int pos = 0;
        final GroupIndex tmpIndex = new GroupIndex(group, indexName, 0, unique, Index.KEY_CONSTRAINT, joinType);
        boolean complete = false;
        try {
            for(String tableCol : tableColPairs) {
                int period = tableCol.indexOf('.');
                if(period == -1) {
                    throw new WrongNameFormatException (tableCol);
                }
                final String tableName = tableCol.substring(0, period).trim();
                final String columnName = tableCol.substring(period+1).trim();
                final UserTable table = findTableInGroup(ais, group, tableName);
                if(table == null) {
                    throw new NoSuchTableException ("", tableName);
                }
                final Column column = table.getColumn(columnName);
                if(column == null) {
                    throw new NoSuchColumnException (columnName);
                }
                tmpIndex.addColumn(new IndexColumn(tmpIndex, column, pos++, true, null));
            }
            complete = true;
        }
        finally {
            // Some ITs create broken indexes directly rather than on a copy of the AIS.
            // Do enough cleanup to keep tables from pointing to half-done index.
            if (!complete)
                tmpIndex.disassociate();
        }

        return tmpIndex;
    }

    private static UserTable findTableInGroup(AkibanInformationSchema ais, Group group, String tableName) {
        for(UserTable table : ais.getUserTables().values()) {
            if(table.getGroup().equals(group) && table.getName().getTableName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }
}
