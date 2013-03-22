
package com.akiban.ais.model;

final class GroupIndexHelper {

    // for use by package

    static void actOnGroupIndexTables(GroupIndex index, IndexColumn indexColumn, IndexAction action) {
        if (!indexColumn.getIndex().equals(index)) {
            throw new IllegalArgumentException("indexColumn must belong to index: " + indexColumn + "not of " + index);
        }
        UserTable userTable = indexColumn.getColumn().getUserTable();
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
        void act(GroupIndex groupIndex, UserTable onTable);
    }

    // class state

    final static IndexAction REMOVE = new IndexAction() {
        @Override
        public void act(GroupIndex groupIndex, UserTable onTable) {
            UserTable ancestor = onTable;
            while(ancestor != null) {
                ancestor.removeGroupIndex(groupIndex);
                ancestor = ancestor.parentTable();
            }
        }

        @Override
        public String toString() {
            return "REMOVE";
        }
    };

    final static IndexAction ADD = new IndexAction() {
        @Override
        public void act(GroupIndex groupIndex, UserTable onTable) {
            UserTable ancestor = onTable;
            while(ancestor != null) {
                ancestor.addGroupIndex(groupIndex);
                ancestor = ancestor.parentTable();
            }
        }

        @Override
        public String toString() {
            return "ADD";
        }
    };
}
