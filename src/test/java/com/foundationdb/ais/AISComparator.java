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

package com.foundationdb.ais;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;

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
                     lhs.getCharsetName(), rhs.getCharsetName());
        assertEquals(realPrefix + "AIS collations",
                     lhs.getCollationName(), rhs.getCollationName());

        GroupMaps lhsGroups = new GroupMaps(lhs.getGroups().values(), withIDs);
        GroupMaps rhsGroups = new GroupMaps(rhs.getGroups().values(), withIDs);
        lhsGroups.compareAndAssert(realPrefix, rhsGroups);

        TableMaps lhsTables = new TableMaps(lhs.getTables().values(), withIDs);
        TableMaps rhsTables = new TableMaps(rhs.getTables().values(), withIDs);
        lhsTables.compareAndAssert(realPrefix, rhsTables);
    }

    private static class GroupMaps {
        public final Collection<TableName> names = new TreeSet<>();
        public final Collection<String> indexes = new TreeSet<>();

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
        public final Collection<String> names = new TreeSet<>();
        public final Collection<String> indexes = new TreeSet<>();
        public final Collection<String> columns = new TreeSet<>();
        public final Collection<String> charAndCols = new TreeSet<>();

        public TableMaps(Collection<Table> tables, boolean withIDs) {
            for(Table table : tables) {
                names.add(table.getName().toString() + (withIDs ? table.getTableId() : ""));
                for(Column column : table.getColumnsIncludingInternal()) {
                    columns.add(column.toString() + " " + column.getTypeDescription() + " " + column.getCharsetName() + "/" + column.getCollationName());
                }
                for(Index index : table.getIndexesIncludingInternal()) {
                    indexes.add(index.toString() + (withIDs ? index.getIndexId() : ""));
                }
                charAndCols.add(table.getName() + " " + table.getDefaultedCharsetName() + "/" + table.getDefaultedCollationName());
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
