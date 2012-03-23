/**
 * Copyright (C) 2012 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
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
        public final Collection<String> names = new TreeSet<String>();
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
                for(Column column : table.getColumns()) {
                    columns.add(column.toString() + " " + column.getTypeDescription() + " " + column.getCharsetAndCollation());
                }
                for(Index index : table.getIndexes()) {
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
