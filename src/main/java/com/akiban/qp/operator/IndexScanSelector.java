/**
 * Copyright (C) 2011 Akiban Technologies Inc.
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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class IndexScanSelector {

    public abstract boolean matchesAll();
    public abstract boolean matches(long map);

    public static IndexScanSelector leftJoinAfter(Index index, final UserTable leafmostRequired) {
        final int leafmostRequiredDepth = leafmostRequired.getDepth();
        return create(index, new Policy() {
            @Override
            public boolean include(UserTable table) {
                return table.getDepth() <= leafmostRequiredDepth;
            }

            @Override
            public String description() {
                return " INNER JOIN thru " + leafmostRequired.getName().getTableName() + ", then LEFT";
            }
        });
    }

    public static IndexScanSelector rightJoinUntil(Index index, final UserTable rootmostRequired) {
        final int leafmostRequiredDepth = rootmostRequired.getDepth();
        return create(index, new Policy() {
            @Override
            public boolean include(UserTable table) {
                return table.getDepth() >= leafmostRequiredDepth;
            }

            @Override
            public String description() {
                return " RIGHT JOIN thru " + rootmostRequired.getName().getTableName() + ", then INNER";
            }
        });

    }

    public static IndexScanSelector inner(Index index) {
        return create(index, new Policy() {
            @Override
            public boolean include(UserTable table) {
                return true;
            }

            @Override
            public String description() {
                return "INNER";
            }
        });
    }

    private static IndexScanSelector create(Index index, Policy policy) {
        if (index.isTableIndex()) {
            return ALLOW_ALL;
        }
        return create((GroupIndex)index, policy);
    }

    private static IndexScanSelector create(GroupIndex index, Policy policy) {
        UserTable giLeaf = index.leafMostTable();
        List<UserTable> requiredTables = new ArrayList<UserTable>(giLeaf.getDepth());
        for(UserTable table = giLeaf, end = index.rootMostTable().parentTable();
            table == null || !table.equals(end);
            table = table.parentTable()
        ) {
            if (table == null || table.parentTable() == giLeaf)
                throw new IllegalArgumentException(table + " isn't in index" + index);
            if (policy.include(table))
                requiredTables.add(table);
        }
        return new SelectiveGiSelector(index, requiredTables, policy.description());
    }

    private interface Policy {
        boolean include(UserTable table);
        String description();
    }

    private IndexScanSelector() {}

    private static final IndexScanSelector ALLOW_ALL = new AllSelector();

    public abstract String describe();

    private static class SelectiveGiSelector extends IndexScanSelector {
        @Override
        public boolean matchesAll() {
            return false;
        }

        @Override
        public boolean matches(long map) {
            return (map & requiredMap) == requiredMap;
        }

        @Override
        public String describe() {
            return description;
        }

        private SelectiveGiSelector(GroupIndex index, Collection<? extends UserTable> tables, String description) {
            long tmpMap = 0;
            for (UserTable table : tables) {
                tmpMap |= (1 << distanceFromLeaf(index, table));
            }
            requiredMap = tmpMap;
            this.description = description;
        }

        private long distanceFromLeaf(GroupIndex index, UserTable table) {
            return index.leafMostTable().getDepth() - table.getDepth();
        }

        private final long requiredMap;
        private final String description;
    }

    private static class AllSelector extends IndexScanSelector {
        @Override
        public boolean matchesAll() {
            return true;
        }

        @Override
        public boolean matches(long map) {
            return true;
        }

        @Override
        public String describe() {
            return "";
        }
    }
}
