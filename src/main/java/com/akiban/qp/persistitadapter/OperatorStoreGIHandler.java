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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexRowComposition;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.Row;
import com.akiban.server.FieldDef;
import com.akiban.server.RowDef;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class OperatorStoreGIHandler implements OperatorStore.GroupIndexHandler<PersistitException> {

    // GroupIndexHandler interface

    @Override
    public void handleRow(GroupIndex groupIndex, Row row)
    throws PersistitException
    {
        assert Action.BULK_ADD.equals(action.action()) == (action.sourceTable()==null) : null;
        UserTable sourceTable = action.sourceTable();
        GroupIndexPosition sourceRowPosition = positionWithinBranch(groupIndex, sourceTable);
        if (sourceRowPosition.equals(GroupIndexPosition.BELOW_SEGMENT)) { // asserts sourceRowPosition != null :-)
            return; // nothing to do
        }

        Exchange exchange = adapter.takeExchange(groupIndex);
        Key key = exchange.getKey();
        key.clear();
        IndexRowComposition irc = groupIndex.indexRowComposition();

        // nullPoint is the point at which we should stop nulling hkey values; needs a better name.
        // This is the last index of the hkey component that should be nulled.
        int nullPoint = -1;
        for(int i=0, LEN = irc.getLength(); i < LEN; ++i) {
            assert irc.isInRowData(i);
            assert ! irc.isInHKey(i);

            final int flattenedIndex = irc.getFieldPosition(i);
            Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);
            Object value = row.field(flattenedIndex, UndefBindings.only());
            RowDef rowDef = (RowDef) column.getTable().rowDef();
            FieldDef fieldDef = rowDef.getFieldDef(column.getPosition());
            fieldDef.getEncoding().toKey(fieldDef, value, key);
            boolean isHKeyComponent = i+1 > groupIndex.getColumns().size();
            if (sourceRowPosition.isAboveSegment() && isHKeyComponent && column.getTable().equals(sourceTable)) {
                nullPoint = i;
            }
        }

        if (!Action.BULK_ADD.equals(action.action()) && sourceRowPosition.isAboveSegment() && nullPoint < 0) {
            return;
        }

        int rightmostTableDepth = depthFromHKey(groupIndex, row);
        exchange.getValue().clear();
        exchange.getValue().put(rightmostTableDepth);

        switch (action.action()) {
        case BULK_ADD:
            assert nullPoint < 0 : nullPoint;
            exchange.store();
            break;
        case STORE:
            exchange.store();
            if (nullOutHKey(nullPoint, groupIndex, row, key)) {
                exchange.remove();
            }
            break;
        case DELETE:
            exchange.remove();
            if (nullOutHKey(nullPoint, groupIndex, row, key)) {
                exchange.store();
            }
            break;
        default:
            throw new UnsupportedOperationException(action.action().name());
        }
    }

    // class interface

    public static OperatorStoreGIHandler forInserting(PersistitAdapter adapter, UserTable userTable) {
        return new OperatorStoreGIHandler(adapter, new RowAction(userTable, Action.STORE));
    }

    public static OperatorStoreGIHandler forRemoving(PersistitAdapter adapter, UserTable userTable) {
        return new OperatorStoreGIHandler(adapter, new RowAction(userTable, Action.DELETE));
    }

    public static OperatorStoreGIHandler forBuilding(PersistitAdapter adapter) {
        return new OperatorStoreGIHandler(adapter, RowAction.FOR_BULK);
    }

    // for use in this class

    private static int depthFromHKey(GroupIndex groupIndex, Row row) {
        final int targetSegments = row.hKey().segments();
        for(UserTable table=groupIndex.leafMostTable(), END=groupIndex.rootMostTable().parentTable();
                !(table == null || table.equals(END));
                table = table.parentTable()
        ){
            if (table.hKey().segments().size() == targetSegments) {
                return table.getDepth();
            }
        }
        throw new AssertionError(
                String.format("couldn't find a table with %d segments for row %s (hkey=%s) in group index %s",
                        targetSegments, row, row.hKey(), groupIndex
                )
        );
    }

    private static GroupIndexPosition positionWithinBranch(GroupIndex groupIndex, UserTable table) {
        final UserTable leafMost = groupIndex.leafMostTable();
        if (table == null) {
            return GroupIndexPosition.ABOVE_SEGMENT;
        }
        else if (table.equals(leafMost)) {
            return GroupIndexPosition.WITHIN_SEGMENT;
        }
        else if (table.isDescendantOf(leafMost)) {
            return GroupIndexPosition.BELOW_SEGMENT;
        }
        else if (groupIndex.rootMostTable().equals(table)) {
            return GroupIndexPosition.WITHIN_SEGMENT;
        }
        else {
            return groupIndex.rootMostTable().isDescendantOf(table)
                    ? GroupIndexPosition.ABOVE_SEGMENT
                    : GroupIndexPosition.WITHIN_SEGMENT;
        }
    }

    private static boolean nullOutHKey(int nullPoint, GroupIndex groupIndex, Row row, Key key) {
        if (nullPoint < 0) {
            return false;
        }
        key.setDepth(nullPoint);
        IndexRowComposition irc = groupIndex.indexRowComposition();
        for (int i = groupIndex.getColumns().size(), LEN=irc.getLength(); i < LEN; ++i) {
            if (i <= nullPoint) {
                key.append(null);
            }
            else {
                final int flattenedIndex = irc.getFieldPosition(i);
                Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);
                Object value = row.field(flattenedIndex, UndefBindings.only());
                RowDef rowDef = (RowDef) column.getTable().rowDef();
                FieldDef fieldDef = rowDef.getFieldDef(column.getPosition());
                fieldDef.getEncoding().toKey(fieldDef, value, key);
            }
        }
        return true;
    }

    public OperatorStoreGIHandler(PersistitAdapter adapter, RowAction action) {
        this.adapter = adapter;
        this.action = action;
    }

    // object state

    private final PersistitAdapter adapter;
    private final RowAction action;

    // nested classes
    enum GroupIndexPosition {
        ABOVE_SEGMENT,
        BELOW_SEGMENT,
        WITHIN_SEGMENT
        ;

        public boolean isAboveSegment() { // more readable shorthand
            return this == ABOVE_SEGMENT;
        }
    }

    private static class RowAction {

        public UserTable sourceTable() {
            return sourceTable;
        }

        public Action action() {
            return action;
        }

        public RowAction(UserTable sourceTable, Action action) {
            assert action != null : "action is null";
            assert Action.BULK_ADD.equals(action) == (sourceTable == null)
                    : String.format("(sourceTable=null)==%s but action=%s", sourceTable==null, action);
            this.sourceTable = sourceTable;
            this.action = action;
        }

        @Override
        public String toString() {
            return action().name() + ' ' + sourceTable();
        }

        private final UserTable sourceTable;
        private final Action action;

        private static RowAction FOR_BULK = new RowAction(null, Action.BULK_ADD);
    }

    public enum Action {STORE, DELETE, BULK_ADD }
}
