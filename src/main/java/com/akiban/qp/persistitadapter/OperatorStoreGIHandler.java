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
import com.akiban.server.KeyConversionTarget;
import com.akiban.server.RowDef;
import com.akiban.server.types.ConversionSource;
import com.akiban.util.ArgumentValidation;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class OperatorStoreGIHandler {

    // GroupIndexHandler interface

    public void handleRow(GroupIndex groupIndex, Row row, Action action, KeyConversionTarget target)
    throws PersistitException
    {
        assert Action.BULK_ADD.equals(action) == (sourceTable==null) : null;
        GroupIndexPosition sourceRowPosition = positionWithinBranch(groupIndex, sourceTable);
        if (sourceRowPosition.equals(GroupIndexPosition.BELOW_SEGMENT)) { // asserts sourceRowPosition != null :-)
            return; // nothing to do
        }

        Exchange exchange = adapter.takeExchange(groupIndex);
        Key key = exchange.getKey();
        key.clear();
        target.attach(key);
        IndexRowComposition irc = groupIndex.indexRowComposition();

        // nullPoint is the point at which we should stop nulling hkey values; needs a better name.
        // This is the last index of the hkey component that should be nulled.
        int nullPoint = -1;

        for(int i=0, LEN = irc.getLength(); i < LEN; ++i) {
            assert irc.isInRowData(i);
            assert ! irc.isInHKey(i);

            final int flattenedIndex = irc.getFieldPosition(i);
            Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);

            ConversionSource source = row.conversionSource(flattenedIndex, UndefBindings.only());
            column.getType().akType().convert(source, target);

            boolean isHKeyComponent = i+1 > groupIndex.getColumns().size();
            if (sourceRowPosition.isAboveSegment() && isHKeyComponent && column.getTable().equals(sourceTable)) {
                nullPoint = i;
            }
        }

        if (!Action.BULK_ADD.equals(action) && sourceRowPosition.isAboveSegment() && nullPoint < 0) {
            return;
        }

        // Description of group index entry values:
        // The value of each index key is the depth of the leafmost table for which there is an actual row.
        // For instance, if you have a COI group and a group index on (i.sku, o.date), LEFT JOIN semantics mean
        // that an order with no items will have an index key of (null, <date>, <hkey>) with value = depth(o) == 1.
        // If you then give that order an item with a null sku, the index key will still be
        // (null, <date>, <hkey) but its value will be depth(i) == 2.
        // This depth is stored as an int (we don't anticipate any group branches with depth > 2^31-1).
        int rightmostTableDepth = depthFromHKey(groupIndex, row);
        exchange.getValue().clear();
        exchange.getValue().put(rightmostTableDepth);

        switch (action) {
        case BULK_ADD:
            assert nullPoint < 0 : nullPoint;
            storeExchange(groupIndex, exchange);
            break;
        case STORE:
            storeExchange(groupIndex, exchange);
            if (nullOutHKey(nullPoint, groupIndex, row, key)) {
                removeExchange(groupIndex, exchange);
            }
            break;
        case DELETE:
            removeExchange(groupIndex, exchange);
            if (nullOutHKey(nullPoint, groupIndex, row, key)) {
                storeExchange(groupIndex, exchange);
            }
            break;
        default:
            throw new UnsupportedOperationException(action.name());
        }
    }

    // class interface

    public static OperatorStoreGIHandler forTable(PersistitAdapter adapter, UserTable userTable) {
        ArgumentValidation.notNull("userTable", userTable);
        return new OperatorStoreGIHandler(adapter, userTable);
    }

    public static OperatorStoreGIHandler forBuilding(PersistitAdapter adapter) {
        return new OperatorStoreGIHandler(adapter, null);
    }

    // For use within the package

    static void setGiHandlerHook(GIHandlerHook newHook) {
        OperatorStoreGIHandler.giHandlerHook = newHook;
    }

    // for use in this class

    private void storeExchange(GroupIndex groupIndex, Exchange exchange) throws PersistitException {
        exchange.store();
        if (giHandlerHook != null) {
            giHandlerHook.storeHook(groupIndex, exchange.getKey(), exchange.getValue().get());
        }
    }

    private void removeExchange(GroupIndex groupIndex, Exchange exchange) throws PersistitException {
        exchange.remove();
        if (giHandlerHook != null) {
            giHandlerHook.removeHook(groupIndex, exchange.getKey());
        }
    }

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

    private OperatorStoreGIHandler(PersistitAdapter adapter, UserTable sourceTable) {
        this.adapter = adapter;
        this.sourceTable = sourceTable;
    }

    // object state

    private final PersistitAdapter adapter;
    private final UserTable sourceTable;
    private static volatile GIHandlerHook giHandlerHook;

    // nested classes

    interface GIHandlerHook {
        void storeHook(GroupIndex groupIndex, Key key, Object value);
        void removeHook(GroupIndex groupIndex, Key key);
    }

    enum GroupIndexPosition {
        ABOVE_SEGMENT,
        BELOW_SEGMENT,
        WITHIN_SEGMENT
        ;

        public boolean isAboveSegment() { // more readable shorthand
            return this == ABOVE_SEGMENT;
        }
    }

    static enum Action {STORE, DELETE, BULK_ADD }
}
