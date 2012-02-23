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

package com.akiban.ais.model;

final class GroupIndexHelper {

    // for use by package

    static void actOnGroupIndexTables(GroupIndex index, IndexColumn indexColumn, IndexAction action) {
        if (!indexColumn.getIndex().equals(index)) {
            throw new IllegalArgumentException("indexColumn must belong to index: " + indexColumn + "not of " + index);
        }
        Table userTable = indexColumn.getColumn().getTable();
        assert userTable.isUserTable() : "not a user table: " + userTable;
        action.act(index, userTable);
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
            onTable.removeGroupIndex(groupIndex);
        }

        @Override
        public String toString() {
            return "REMOVE";
        }
    };

    final static IndexAction ADD = new IndexAction() {
        @Override
        public void act(GroupIndex groupIndex, Table onTable) {
            onTable.addGroupIndex(groupIndex);
        }

        @Override
        public String toString() {
            return "ADD";
        }
    };
}
