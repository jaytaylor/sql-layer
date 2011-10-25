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
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class IndexScanSelector {

    public abstract boolean matchesAll();
    public abstract boolean matches(long map);

    public static IndexScanSelector innerUntil(Index index, UserTable leafmostRequired) {
        if (index.isTableIndex()) {
            assert index.leafMostTable().equals(leafmostRequired) : leafmostRequired + " not in " + index;
            return ALLOW_ALL;
        }
        else {
            Table rootmost = index.rootMostTable();
            Table leafmost = index.leafMostTable();
            List<UserTable> requiredTables = new ArrayList<UserTable>(leafmostRequired.getDepth());
            for (UserTable required = leafmostRequired; ; required = required.parentTable()) {
                if (required == null || required.parentTable() == leafmost)
                    throw new IllegalArgumentException(required + " isn't in index" + index);
                requiredTables.add(required);
                if (required.equals(rootmost))
                    break;
            }
            final String description;
            description = leafmostRequired != index.leafMostTable()
                    ? " INNER JOIN thru " + leafmostRequired.getName().getTableName()
                    : "";
            return new SelectiveGiSelector((GroupIndex)index, requiredTables, description);
        }
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
