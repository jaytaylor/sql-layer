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

final class GroupIndexHelper {

    // for use by package

    static void actOnGroupIndexTables(GroupIndex index, IndexColumn indexColumn, IndexAction action) {
        if (!indexColumn.getIndex().equals(index)) {
            throw new IllegalArgumentException("indexColumn must belong to index: " + indexColumn + "not of " + index);
        }
        Table table = indexColumn.getColumn().getTable();
        action.act(index, table);
    }

    static void actOnGroupIndexTables(GroupIndex index, IndexAction action) {
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            actOnGroupIndexTables(index, indexColumn, action);
        }
    }

    // nested classes
    private static interface IndexAction {
        void act(GroupIndex groupIndex, Table onTable);
    }

    // class state

    final static IndexAction REMOVE = new IndexAction() {
        @Override
        public void act(GroupIndex groupIndex, Table onTable) {
            Table ancestor = onTable;
            while(ancestor != null) {
                ancestor.removeGroupIndex(groupIndex);
                ancestor = ancestor.getParentTable();
            }
        }

        @Override
        public String toString() {
            return "REMOVE";
        }
    };

    final static IndexAction ADD = new IndexAction() {
        @Override
        public void act(GroupIndex groupIndex, Table onTable) {
            Table ancestor = onTable;
            while(ancestor != null) {
                ancestor.addGroupIndex(groupIndex);
                ancestor = ancestor.getParentTable();
            }
        }

        @Override
        public String toString() {
            return "ADD";
        }
    };
}
