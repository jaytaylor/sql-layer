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

package com.akiban.ais;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;

import java.util.Collection;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class AISComparator {
    public static void compareAndAssert(AkibanInformationSchema lhs, AkibanInformationSchema rhs, boolean withIDs) {
        compareAndAssert("", lhs, rhs, withIDs);
    }
    
    public static void compareAndAssert(String msgPrefix, AkibanInformationSchema lhs, AkibanInformationSchema rhs, boolean withIDs) {
        String realPrefix = msgPrefix.length() > 0 ? msgPrefix + ": " : "";
        
        assertEquals(realPrefix + "AIS charsets",
                     lhs.getCharsetAndCollation().charset(), rhs.getCharsetAndCollation().charset());
        assertEquals(realPrefix + "AIS collations",
                     lhs.getCharsetAndCollation().collation(), rhs.getCharsetAndCollation().collation());

        GroupMaps lhsGroups = new GroupMaps(lhs.getGroups().values(), withIDs);
        GroupMaps rhsGroups = new GroupMaps(rhs.getGroups().values(), withIDs);
        lhsGroups.compareAndAssert(realPrefix, rhsGroups);

        TableMaps lhsTables = new TableMaps(lhs.getUserTables().values(), withIDs);
        TableMaps rhsTables = new TableMaps(rhs.getUserTables().values(), withIDs);
        lhsTables.compareAndAssert(realPrefix, rhsTables);
    }

    private static class GroupMaps {
        public final Collection<TableName> names = new TreeSet<TableName>();
        public final Collection<String> indexes = new TreeSet<String>();

        public GroupMaps(Collection<Group> groups, boolean withIDs) {
            for(Group group : groups) {
                names.add(group.getName());
                for(Index index : group.getIndexes()) {
                    indexes.add(index.toString() + (withIDs ? index.getIndexId() : ""));
                }
            }
        }

        public void compareAndAssert(String msgPrefix, GroupMaps rhs) {
            assertEquals(msgPrefix + "Group names", names.toString(), rhs.names.toString());
            assertEquals(msgPrefix + "Group indexes", indexes.toString(), rhs.indexes.toString());
        }
    }

    private static class TableMaps {
        public final Collection<String> names = new TreeSet<String>();
        public final Collection<String> indexes = new TreeSet<String>();
        public final Collection<String> columns = new TreeSet<String>();
        public final Collection<String> charAndCols = new TreeSet<String>();

        public TableMaps(Collection<UserTable> tables, boolean withIDs) {
            for(UserTable table : tables) {
                names.add(table.getName().toString() + (withIDs ? table.getTableId() : ""));
                for(Column column : table.getColumnsIncludingInternal()) {
                    columns.add(column.toString() + " " + column.getTypeDescription() + " " + column.getCharsetAndCollation());
                }
                for(Index index : table.getIndexesIncludingInternal()) {
                    indexes.add(index.toString() + (withIDs ? index.getIndexId() : ""));
                }
                charAndCols.add(table.getName() + " " + table.getCharsetAndCollation().toString());
            }
        }

        public void compareAndAssert(String msgPrefix, TableMaps rhs) {
            assertEquals(msgPrefix + "Table names", names.toString(), rhs.names.toString());
            assertEquals(msgPrefix + "Table columns", columns.toString(), rhs.columns.toString());
            assertEquals(msgPrefix + "Table indexes", indexes.toString(), rhs.indexes.toString());
            assertEquals(msgPrefix + "Table charAndCols", charAndCols.toString(), rhs.charAndCols.toString());
        }
    }
}
