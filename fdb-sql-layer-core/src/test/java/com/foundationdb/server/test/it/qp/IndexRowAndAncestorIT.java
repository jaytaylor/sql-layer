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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.TableRowType;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.qp.operator.API.ancestorLookup_Default;
import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertEquals;

// Inspired by bug 987942

public class IndexRowAndAncestorIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        // A bizarre coih schema in which all FKs are PKs. I.e., all joins are 1:1.
        c = createTable(
            "s", "c",
            "id int not null",
            "cx int not null",
            "primary key(id)");
        o = createTable(
            "s", "o",
            "id int not null",
            "ox int",
            "primary key(id)",
            "grouping foreign key (id) references c(id)");
        i = createTable(
            "s", "i",
            "id int not null",
            "ix int",
            "primary key(id)",
            "grouping foreign key (id) references o(id)");
        h = createTable(
            "s", "h",
            "id int not null",
            "hx int",
            "primary key(id)",
            "grouping foreign key (id) references i(id)");
        idxH = createIndex("s", "h", "idxH", "hx");
        TableName groupName = new TableName("s", "c");
        // ih left/right indexes declare an hkey column from the leafmost table
        idxIHLeft = createLeftGroupIndex(groupName, "idxIHLeft", "i.ix", "h.hx", "h.id");
        idxIHRight = createRightGroupIndex(groupName, "idxIHRight", "i.ix", "h.hx", "h.id");
        // oh left/right indexes declare an hkey column from the rootmost table
        idxOHLeft = createLeftGroupIndex(groupName, "idxOHLeft", "o.ox", "i.ix", "h.hx", "o.id");
        idxOHRight = createRightGroupIndex(groupName, "idxOHRight", "o.ox", "i.ix", "h.hx", "o.id");
        // ch left/right indexes declare an hky column from an internal table (neither leafmost nor rootmost)
        idxCHLeft = createLeftGroupIndex(groupName, "idxCHLeft", "c.cx", "o.ox", "i.ix", "h.hx", "o.id");
        idxCHRight = createRightGroupIndex(groupName, "idxCHRight", "c.cx", "o.ox", "i.ix", "h.hx", "o.id");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        cRowType = schema.tableRowType(table(c));
        oRowType = schema.tableRowType(table(o));
        iRowType = schema.tableRowType(table(i));
        hRowType = schema.tableRowType(table(h));
        hIndexRowType = indexType(h, "hx");
        ihLeftIndexRowType = groupIndexType(Index.JoinType.LEFT, "i.ix", "h.hx", "h.id");
        ihRightIndexRowType = groupIndexType(Index.JoinType.RIGHT, "i.ix", "h.hx", "h.id");
        ohLeftIndexRowType = groupIndexType(Index.JoinType.LEFT, "o.ox", "i.ix", "h.hx", "o.id");
        ohRightIndexRowType = groupIndexType(Index.JoinType.RIGHT, "o.ox", "i.ix", "h.hx", "o.id");
        chLeftIndexRowType = groupIndexType(Index.JoinType.LEFT, "c.cx", "o.ox", "i.ix", "h.hx", "o.id");
        chRightIndexRowType = groupIndexType(Index.JoinType.RIGHT, "c.cx", "o.ox", "i.ix", "h.hx", "o.id");
        cOrdinal = ddl().getTable(session(), c).getOrdinal();
        oOrdinal = ddl().getTable(session(), o).getOrdinal();
        iOrdinal = ddl().getTable(session(), i).getOrdinal();
        hOrdinal = ddl().getTable(session(), h).getOrdinal();
        group = group(c);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        loadDatabase();
    }

    private void loadDatabase()
    {
        db = new Row[] {
            // 1: c
            row(c, 1L, 19999L),
            // 2: c-o
            row(c, 2L, 29999L),
            row(o, 2L, 209999),
            // 3: c-i
            row(c, 3L, 39999L),
            row(o, 3L, 309999L),
            row(i, 3L, 3009999L),
            // 4: c-h
            row(c, 4L, 49999L),
            row(o, 4L, 409999L),
            row(i, 4L, 4009999L),
            row(h, 4L, 40009999L),
            // 5: o-h
            row(o, 5L, 509999L),
            row(i, 5L, 5009999L),
            row(h, 5L, 50009999L),
            // 6: i-h
            row(i, 6L, 6009999L),
            row(h, 6L, 60009999L),
            // 7: h
            row(h, 7L, 70009999L),
        };
        use(db);
    }

    @Test
    public void testHKeyMetadata()
    {
        HKey hKey;
        // h HKey: [C, H.id, O, I, H]
        hKey = hRowType.table().hKey();
        assertEquals(1, hKey.nColumns());
        assertEquals("s.h.id", hKey.column(0).getDescription());
        // i HKey: [C, I.id, O, I]
        hKey = iRowType.table().hKey();
        assertEquals(1, hKey.nColumns());
        assertEquals("s.i.id", hKey.column(0).getDescription());
        // o HKey: [C, O.id, O]
        hKey = oRowType.table().hKey();
        assertEquals(1, hKey.nColumns());
        assertEquals("s.o.id", hKey.column(0).getDescription());
        // c HKey: [C, C.id]
        hKey = cRowType.table().hKey();
        assertEquals(1, hKey.nColumns());
        assertEquals("s.c.id", hKey.column(0).getDescription());
    }

    @Test
    public void testIndexRowMetadata()
    {
        IndexRowComposition irc;
        // H(hx) index row:
        //     declared: H.hx
        //     hkey: H.id
        irc = hIndexRowType.index().indexRowComposition();
        assertEquals(2, irc.getLength());
        assertEquals(1, irc.getFieldPosition(0));
        assertEquals(0, irc.getFieldPosition(1));
        // Field numbers for flattened row:
        // c:
        //     0: id
        //     1: cx
        // o:
        //     2: id
        //     3: ox
        // i:
        //     4: id
        //     5: ix
        // h:
        //     6: id
        //     7: hx
        // (i.ix, h.hx, h.id) left join index row:
        //     declared: I.ix  H.hx  H.id
        //     hkey: I.id
        irc = ihLeftIndexRowType.index().indexRowComposition();
        assertEquals(4, irc.getLength());
        assertEquals(5, irc.getFieldPosition(0));
        assertEquals(7, irc.getFieldPosition(1));
        assertEquals(6, irc.getFieldPosition(2));
        assertEquals(4, irc.getFieldPosition(3));
        // (i.ix, h.hx, h.id) right join index row:
        //     declared: I.ix  H.hx  H.id
        //     hkey:
        irc = ihRightIndexRowType.index().indexRowComposition();
        assertEquals(3, irc.getLength());
        assertEquals(5, irc.getFieldPosition(0));
        assertEquals(7, irc.getFieldPosition(1));
        assertEquals(6, irc.getFieldPosition(2));
        // (o.ox, i.ix, h.hx, o.id) left join index row:
        //     declared: O.ox  I.ix  H.hx  o.id
        //     hkey:
        irc = ohLeftIndexRowType.index().indexRowComposition();
        assertEquals(4, irc.getLength());
        assertEquals(3, irc.getFieldPosition(0));
        assertEquals(5, irc.getFieldPosition(1));
        assertEquals(7, irc.getFieldPosition(2));
        assertEquals(2, irc.getFieldPosition(3));
        // (o.ox, i.ix, h.hx, o.id) right join index row:
        //     declared: O.ox  I.ix  H.hx  O.id
        //     hkey: H.id
        irc = ohRightIndexRowType.index().indexRowComposition();
        assertEquals(5, irc.getLength());
        assertEquals(3, irc.getFieldPosition(0));
        assertEquals(5, irc.getFieldPosition(1));
        assertEquals(7, irc.getFieldPosition(2));
        assertEquals(2, irc.getFieldPosition(3));
        assertEquals(6, irc.getFieldPosition(4));
        // (c.cx, o.ox, i.ix, h.hx, o.id) left join index row:
        //     declared: C.cx  O.ox  I.ix  H.hx, O.id
        //     hkey: C.id
        irc = chLeftIndexRowType.index().indexRowComposition();
        assertEquals(6, irc.getLength());
        assertEquals(1, irc.getFieldPosition(0));
        assertEquals(3, irc.getFieldPosition(1));
        assertEquals(5, irc.getFieldPosition(2));
        assertEquals(7, irc.getFieldPosition(3));
        assertEquals(2, irc.getFieldPosition(4));
        assertEquals(0, irc.getFieldPosition(5));
        // (c.cx, o.ox, i.ix, h.hx, o.id) right join index row:
        //     declared: C.cx  O.ox  I.ix  H.hx, O.id
        //     hkey: H.id
        irc = chRightIndexRowType.index().indexRowComposition();
        assertEquals(6, irc.getLength());
        assertEquals(1, irc.getFieldPosition(0));
        assertEquals(3, irc.getFieldPosition(1));
        assertEquals(5, irc.getFieldPosition(2));
        assertEquals(7, irc.getFieldPosition(3));
        assertEquals(2, irc.getFieldPosition(4));
        assertEquals(6, irc.getFieldPosition(5));
    }

    @Test
    public void testIndexToHKeyMetadata()
    {
        // H(hx) index row:
        //     declared: H.hx
        //     hkey: H.id
        // H hkey: [C, H.id, O, I, H]
        TableIndex tableIndex = (TableIndex) hIndexRowType.index();
        IndexToHKey ih = tableIndex.indexToHKey();
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(1, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // (i.ix, h.hx, h.id) left join index row:
        //     declared: I.ix  H.hx, H.id
        //     hkey: I.id
        GroupIndex groupIndex = (GroupIndex) ihLeftIndexRowType.index();
        // H hkey: [C, H.id, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(2, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, I.id, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, I.id, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, I.id]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        // (i.ix, h.hx, h.id) right join index row:
        //     declared: I.ix  H.hx, H.id
        //     hkey:
        groupIndex = (GroupIndex) ihRightIndexRowType.index();
        // H hkey: [C, H.id, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(2, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, H.id, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(2, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, H.id, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(2, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, H.id]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(2, ih.getIndexRowPosition(1));
        // (o.ox, i.ix, h.hx, o.id) left join index row:
        //     declared: O.ox  I.ix  H.hx, O.id
        //     hkey:
        groupIndex = (GroupIndex) ohLeftIndexRowType.index();
        // H hkey: [C, O.id, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, O.id, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, O.id, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, O.id]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        // (o.ox, i.ix, h.hx, o.id) right join index row:
        //     declared: O.ox  I.ix  H.hx, O.id
        //     hkey: H.id
        groupIndex = (GroupIndex) ohRightIndexRowType.index();
        // H hkey: [C, H.id, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, I.id, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, O.id, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, O.id]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        // (c.cx, o.ox, i.ix, h.hx, o.id) left join index row:
        //     declared: C.cx  O.ox  I.ix  H.hx  O.id
        //     hkey: C.id
        groupIndex = (GroupIndex) chLeftIndexRowType.index();
        // H hkey: [C, O.id, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, O.id, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, O.id, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, C.id]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(5, ih.getIndexRowPosition(1));
        // (c.cx, o.ox, i.ix, h.hx, o.id) right join index row:
        //     declared: C.cx  O.ox  I.ix  H.hx  O.id
        //     hkey: H.id
        groupIndex = (GroupIndex) chRightIndexRowType.index();
        // H hkey: [C, H.id, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(5, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, H.id, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(5, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, O.id, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, O.id]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
    }

    @Test
    public void testH()
    {
        Operator plan =
            ancestorLookup_Default(
                indexScan_Default(
                    hIndexRowType,
                    IndexKeyRange.unbounded(hIndexRowType),
                    new API.Ordering()),
                group,
                hIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            row(cRowType, 4L, 49999L),
            row(oRowType, 4L, 409999L),
            row(iRowType, 4L, 4009999L),
            row(hRowType, 4L, 40009999L),
            row(oRowType, 5L, 509999L),
            row(iRowType, 5L, 5009999L),
            row(hRowType, 5L, 50009999L),
            row(iRowType, 6L, 6009999L),
            row(hRowType, 6L, 60009999L),
            row(hRowType, 7L, 70009999L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testIHLeft()
    {
        Operator plan =
            ancestorLookup_Default(
                indexScan_Default(
                    ihLeftIndexRowType,
                    IndexKeyRange.unbounded(ihLeftIndexRowType),
                    new API.Ordering(),
                    IndexScanSelector.leftJoinAfter(ihLeftIndexRowType.index(), iRowType.table())),
                group,
                ihLeftIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            row(cRowType, 3L, 39999L),
            row(oRowType, 3L, 309999L),
            row(iRowType, 3L, 3009999L),
            row(cRowType, 4L, 49999L),
            row(oRowType, 4L, 409999L),
            row(iRowType, 4L, 4009999L),
            row(hRowType, 4L, 40009999L),
            row(oRowType, 5L, 509999L),
            row(iRowType, 5L, 5009999L),
            row(hRowType, 5L, 50009999L),
            row(iRowType, 6L, 6009999L),
            row(hRowType, 6L, 60009999L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testIHRight()
    {
        Operator plan =
            ancestorLookup_Default(
                indexScan_Default(
                    ihRightIndexRowType,
                    IndexKeyRange.unbounded(ihRightIndexRowType),
                    new API.Ordering(),
                    IndexScanSelector.rightJoinUntil(ihRightIndexRowType.index(), hRowType.table())),
                group,
                ihRightIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            // 7 is first because its index key is (null, 70009999)
            row(hRowType, 7L, 70009999L),
            row(cRowType, 4L, 49999L),
            row(oRowType, 4L, 409999L),
            row(iRowType, 4L, 4009999L),
            row(hRowType, 4L, 40009999L),
            row(oRowType, 5L, 509999L),
            row(iRowType, 5L, 5009999L),
            row(hRowType, 5L, 50009999L),
            row(iRowType, 6L, 6009999L),
            row(hRowType, 6L, 60009999L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testOHLeft()
    {
        Operator plan =
            ancestorLookup_Default(
                indexScan_Default(
                    ohLeftIndexRowType,
                    IndexKeyRange.unbounded(ohLeftIndexRowType),
                    new API.Ordering(),
                    IndexScanSelector.leftJoinAfter(ohLeftIndexRowType.index(), oRowType.table())),
                group,
                ohLeftIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            row(cRowType, 2L, 29999L),
            row(oRowType, 2L, 209999L),
            row(cRowType, 3L, 39999L),
            row(oRowType, 3L, 309999L),
            row(iRowType, 3L, 3009999L),
            row(cRowType, 4L, 49999L),
            row(oRowType, 4L, 409999L),
            row(iRowType, 4L, 4009999L),
            row(hRowType, 4L, 40009999L),
            row(oRowType, 5L, 509999L),
            row(iRowType, 5L, 5009999L),
            row(hRowType, 5L, 50009999L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testOHRight()
    {
        Operator plan =
            ancestorLookup_Default(
                indexScan_Default(
                    ohRightIndexRowType,
                    IndexKeyRange.unbounded(ohRightIndexRowType),
                    new API.Ordering(),
                    IndexScanSelector.rightJoinUntil(ohRightIndexRowType.index(), hRowType.table())),
                group,
                ohRightIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            // 7, 6 first due to nulls in the index key
            row(hRowType, 7L, 70009999L),
            row(iRowType, 6L, 6009999L),
            row(hRowType, 6L, 60009999L),
            row(cRowType, 4L, 49999L),
            row(oRowType, 4L, 409999L),
            row(iRowType, 4L, 4009999L),
            row(hRowType, 4L, 40009999L),
            row(oRowType, 5L, 509999L),
            row(iRowType, 5L, 5009999L),
            row(hRowType, 5L, 50009999L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCHLeft()
    {
        Operator plan =
            ancestorLookup_Default(
                indexScan_Default(
                    chLeftIndexRowType,
                    IndexKeyRange.unbounded(chLeftIndexRowType),
                    new API.Ordering(),
                    IndexScanSelector.leftJoinAfter(chLeftIndexRowType.index(), cRowType.table())),
                group,
                chLeftIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            row(cRowType, 1L, 19999L),
            row(cRowType, 2L, 29999L),
            row(oRowType, 2L, 209999L),
            row(cRowType, 3L, 39999L),
            row(oRowType, 3L, 309999L),
            row(iRowType, 3L, 3009999L),
            row(cRowType, 4L, 49999L),
            row(oRowType, 4L, 409999L),
            row(iRowType, 4L, 4009999L),
            row(hRowType, 4L, 40009999L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCHRight()
    {
        Operator plan =
            ancestorLookup_Default(
                indexScan_Default(
                    chRightIndexRowType,
                    IndexKeyRange.unbounded(chRightIndexRowType),
                    new API.Ordering(),
                    IndexScanSelector.rightJoinUntil(chRightIndexRowType.index(), hRowType.table())),
                group,
                chRightIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            // 7, 6, 5 first due to nulls in the index key
            row(hRowType, 7L, 70009999L),
            row(iRowType, 6L, 6009999L),
            row(hRowType, 6L, 60009999L),
            row(oRowType, 5L, 509999L),
            row(iRowType, 5L, 5009999L),
            row(hRowType, 5L, 50009999L),
            row(cRowType, 4L, 49999L),
            row(oRowType, 4L, 409999L),
            row(iRowType, 4L, 4009999L),
            row(hRowType, 4L, 40009999L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private int c;
    private int o;
    private int i;
    private int h;
    private TableIndex idxH;
    private GroupIndex idxIHLeft;
    private GroupIndex idxIHRight;
    private GroupIndex idxOHLeft;
    private GroupIndex idxOHRight;
    private GroupIndex idxCHLeft;
    private GroupIndex idxCHRight;
    private TableRowType cRowType;
    private TableRowType oRowType;
    private TableRowType iRowType;
    private TableRowType hRowType;
    private IndexRowType hIndexRowType;
    private IndexRowType ihLeftIndexRowType;
    private IndexRowType ihRightIndexRowType;
    private IndexRowType ohLeftIndexRowType;
    private IndexRowType ohRightIndexRowType;
    private IndexRowType chLeftIndexRowType;
    private IndexRowType chRightIndexRowType;
    private Group group;
    private int cOrdinal;
    private int oOrdinal;
    private int iOrdinal;
    private int hOrdinal;
}
