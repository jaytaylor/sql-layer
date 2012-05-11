/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.*;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.qp.operator.API.ancestorLookup_Default;
import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static org.junit.Assert.*;

// Inspired by bug 987942

public class IndexRowAndAncestorIT extends OperatorITBase
{
    @Before
    public void before()
    {
        createSchema();
        // loadDatabase();
    }

    private void createSchema()
    {
        // A bizarre coih schema in which all FKs are PKs. I.e., all joins are 1:1.
        c = createTable(
            "s", "c",
            "cid int not null",
            "cx int not null",
            "primary key(cid)");
        o = createTable(
            "s", "o",
            "cid int not null",
            "ox int",
            "primary key(cid)",
            "grouping foreign key (cid) references c(cid)");
        i = createTable(
            "s", "i",
            "cid int not null",
            "ix int",
            "primary key(cid)",
            "grouping foreign key (cid) references o(cid)");
        h = createTable(
            "s", "h",
            "cid int not null",
            "hx int",
            "primary key(cid)",
            "grouping foreign key (cid) references i(cid)");
        idxH = createIndex("s", "h", "idxH", "hx");
        idxIHLeft = createGroupIndex("c", "idxIHLeft", "i.ix, h.hx", Index.JoinType.LEFT);
        idxIHRight = createGroupIndex("c", "idxIHRight", "i.ix, h.hx", Index.JoinType.RIGHT);
        idxOHLeft = createGroupIndex("c", "idxOHLeft", "o.ox, i.ix, h.hx", Index.JoinType.LEFT);
        idxOHRight = createGroupIndex("c", "idxOHRight", "o.ox, i.ix, h.hx", Index.JoinType.RIGHT);
        idxCHLeft = createGroupIndex("c", "idxCHLeft", "c.cx, o.ox, i.ix, h.hx", Index.JoinType.LEFT);
        idxCHRight = createGroupIndex("c", "idxCHRight", "c.cx, o.ox, i.ix, h.hx", Index.JoinType.RIGHT);
        schema = new com.akiban.qp.rowtype.Schema(rowDefCache().ais());
        cRowType = schema.userTableRowType(userTable(c));
        oRowType = schema.userTableRowType(userTable(o));
        iRowType = schema.userTableRowType(userTable(i));
        hRowType = schema.userTableRowType(userTable(h));
        hIndexRowType = indexType(h, "hx");
        ihLeftIndexRowType = groupIndexType(Index.JoinType.LEFT, "i.ix", "h.hx");
        ihRightIndexRowType = groupIndexType(Index.JoinType.RIGHT, "i.ix", "h.hx");
        ohLeftIndexRowType = groupIndexType(Index.JoinType.LEFT, "o.ox", "i.ix", "h.hx");
        ohRightIndexRowType = groupIndexType(Index.JoinType.RIGHT, "o.ox", "i.ix", "h.hx");
        chLeftIndexRowType = groupIndexType(Index.JoinType.LEFT, "c.cx", "o.ox", "i.ix", "h.hx");
        chRightIndexRowType = groupIndexType(Index.JoinType.RIGHT, "c.cx", "o.ox", "i.ix", "h.hx");
        cOrdinal = ddl().getTable(session(), c).rowDef().getOrdinal();
        oOrdinal = ddl().getTable(session(), o).rowDef().getOrdinal();
        iOrdinal = ddl().getTable(session(), i).rowDef().getOrdinal();
        hOrdinal = ddl().getTable(session(), h).rowDef().getOrdinal();
        group = groupTable(c);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    private void loadDatabase()
    {
        db = new NewRow[] {
            // 1: c
            createNewRow(c, 1L, 19999L),
            // 2: c-o
            createNewRow(c, 2L, 29999L),
            createNewRow(o, 2L, 209999),
            // 3: c-i
            createNewRow(c, 3L, 39999L),
            createNewRow(o, 3L, 309999L),
            createNewRow(i, 3L, 3009999L),
            // 4: c-h
            createNewRow(c, 4L, 49999L),
            createNewRow(o, 4L, 409999L),
            createNewRow(i, 4L, 4009999L),
            createNewRow(h, 4L, 40009999L),
            // 5: o-h
            createNewRow(o, 5L, 509999L),
            createNewRow(i, 5L, 5009999L),
            createNewRow(h, 5L, 50009999L),
            // 6: i-h
            createNewRow(i, 6L, 6009999L),
            createNewRow(h, 6L, 60009999L),
            // 7: h
            createNewRow(h, 7L, 70009999L),
        };
        use(db);
    }

    @Test
    public void testHKeyMetadata()
    {
        HKey hKey;
        // h HKey: [C, H.cid, O, I, H]
        hKey = hRowType.userTable().hKey();
        assertEquals(1, hKey.nColumns());
        assertEquals("s.h.cid", hKey.column(0).getDescription());
        // i HKey: [C, I.cid, O, I]
        hKey = iRowType.userTable().hKey();
        assertEquals(1, hKey.nColumns());
        assertEquals("s.i.cid", hKey.column(0).getDescription());
        // o HKey: [C, O.cid, O]
        hKey = oRowType.userTable().hKey();
        assertEquals(1, hKey.nColumns());
        assertEquals("s.o.cid", hKey.column(0).getDescription());
        // c HKey: [C, C.cid]
        hKey = cRowType.userTable().hKey();
        assertEquals(1, hKey.nColumns());
        assertEquals("s.c.cid", hKey.column(0).getDescription());
    }

    @Test
    public void testIndexRowMetadata()
    {
        IndexRowComposition irc;
        // H(hx) index row:
        //     declared: H.hx
        //     leafward hkey: H.cid
        //     rootward hkey: (not relevant for table index)
        irc = hIndexRowType.index().indexRowComposition();
        assertEquals(2, irc.getLength());
        assertEquals(1, irc.getFieldPosition(0));
        assertEquals(0, irc.getFieldPosition(1));
        // Field numbers for flattened row:
        // c:
        //     0: cid
        //     1: cx
        // o:
        //     2: cid
        //     3: ox
        // i:
        //     4: cid
        //     5: ix
        // h:
        //     6: cid
        //     7: hx
        // (ix, hx) index row:
        //     declared: I.ix  H.hx
        //     leafward hkey: H.cid
        //     rootward hkey: I.cid
        GroupIndexRowComposition girc;
        girc = ((GroupIndex)ihLeftIndexRowType.index()).groupIndexRowComposition();
        assertEquals(4, girc.size());
        assertEquals(5, girc.positionInFlattenedRow(0));
        assertEquals(7, girc.positionInFlattenedRow(1));
        assertEquals(6, girc.positionInFlattenedRow(2));
        assertEquals(4, girc.positionInFlattenedRow(3));
        assertNull(girc.equivalentHKeyIndexPositions(0));
        assertNull(girc.equivalentHKeyIndexPositions(1));
        assertNull(girc.equivalentHKeyIndexPositions(2));
        assertArrayEquals(intArray(2), girc.equivalentHKeyIndexPositions(3));
        girc = ((GroupIndex)ihRightIndexRowType.index()).groupIndexRowComposition();
        assertEquals(4, girc.size());
        assertEquals(5, girc.positionInFlattenedRow(0));
        assertEquals(7, girc.positionInFlattenedRow(1));
        assertEquals(6, girc.positionInFlattenedRow(2));
        assertEquals(4, girc.positionInFlattenedRow(3));
        assertNull(girc.equivalentHKeyIndexPositions(0));
        assertNull(girc.equivalentHKeyIndexPositions(1));
        assertNull(girc.equivalentHKeyIndexPositions(2));
        assertArrayEquals(intArray(2), girc.equivalentHKeyIndexPositions(3));
        // (ox, ix, hx) index row:
        //     declared: O.ox  I.ix  H.hx
        //     leafward hkey: H.cid
        //     rootward hkey: O.cid  I.cid
        girc = ((GroupIndex)ohLeftIndexRowType.index()).groupIndexRowComposition();
        assertEquals(6, girc.size());
        assertEquals(3, girc.positionInFlattenedRow(0));
        assertEquals(5, girc.positionInFlattenedRow(1));
        assertEquals(7, girc.positionInFlattenedRow(2));
        assertEquals(6, girc.positionInFlattenedRow(3));
        assertEquals(2, girc.positionInFlattenedRow(4));
        assertEquals(4, girc.positionInFlattenedRow(5));
        assertNull(girc.equivalentHKeyIndexPositions(0));
        assertNull(girc.equivalentHKeyIndexPositions(1));
        assertNull(girc.equivalentHKeyIndexPositions(2));
        assertNull(girc.equivalentHKeyIndexPositions(3));
        assertArrayEquals(intArray(3), girc.equivalentHKeyIndexPositions(4));
        assertArrayEquals(intArray(3, 4), girc.equivalentHKeyIndexPositions(5));
        girc = ((GroupIndex)ohRightIndexRowType.index()).groupIndexRowComposition();
        assertEquals(6, girc.size());
        assertEquals(3, girc.positionInFlattenedRow(0));
        assertEquals(5, girc.positionInFlattenedRow(1));
        assertEquals(7, girc.positionInFlattenedRow(2));
        assertEquals(6, girc.positionInFlattenedRow(3));
        assertEquals(2, girc.positionInFlattenedRow(4));
        assertEquals(4, girc.positionInFlattenedRow(5));
        assertNull(girc.equivalentHKeyIndexPositions(0));
        assertNull(girc.equivalentHKeyIndexPositions(1));
        assertNull(girc.equivalentHKeyIndexPositions(2));
        assertNull(girc.equivalentHKeyIndexPositions(3));
        assertArrayEquals(intArray(3), girc.equivalentHKeyIndexPositions(4));
        assertArrayEquals(intArray(3, 4), girc.equivalentHKeyIndexPositions(5));
        // (cx, ox, ix, hx) index row:
        //     declared: C.cx  O.ox  I.ix  H.hx
        //     leafward hkey: H.cid
        //     rootward hkey: C.cid  O.cid  I.cid
        girc = ((GroupIndex)chLeftIndexRowType.index()).groupIndexRowComposition();
        assertEquals(8, girc.size());
        assertEquals(1, girc.positionInFlattenedRow(0));
        assertEquals(3, girc.positionInFlattenedRow(1));
        assertEquals(5, girc.positionInFlattenedRow(2));
        assertEquals(7, girc.positionInFlattenedRow(3));
        assertEquals(6, girc.positionInFlattenedRow(4));
        assertEquals(0, girc.positionInFlattenedRow(5));
        assertEquals(2, girc.positionInFlattenedRow(6));
        assertEquals(4, girc.positionInFlattenedRow(7));
        assertNull(girc.equivalentHKeyIndexPositions(0));
        assertNull(girc.equivalentHKeyIndexPositions(1));
        assertNull(girc.equivalentHKeyIndexPositions(2));
        assertNull(girc.equivalentHKeyIndexPositions(3));
        assertNull(girc.equivalentHKeyIndexPositions(4));
        assertArrayEquals(intArray(4), girc.equivalentHKeyIndexPositions(5));
        assertArrayEquals(intArray(4, 5), girc.equivalentHKeyIndexPositions(6));
        assertArrayEquals(intArray(4, 5, 6), girc.equivalentHKeyIndexPositions(7));
        girc = ((GroupIndex)chRightIndexRowType.index()).groupIndexRowComposition();
        assertEquals(8, girc.size());
        assertEquals(1, girc.positionInFlattenedRow(0));
        assertEquals(3, girc.positionInFlattenedRow(1));
        assertEquals(5, girc.positionInFlattenedRow(2));
        assertEquals(7, girc.positionInFlattenedRow(3));
        assertEquals(6, girc.positionInFlattenedRow(4));
        assertEquals(0, girc.positionInFlattenedRow(5));
        assertEquals(2, girc.positionInFlattenedRow(6));
        assertEquals(4, girc.positionInFlattenedRow(7));
    }

    @Test
    public void testIndexToHKeyMetadata()
    {
        // H(hx) index row:
        //     declared: H.hx
        //     leafward hkey: H.cid
        //     rootward hkey: (not relevant for table index)
        // H hkey: [C, H.cid, O, I, H]
        TableIndex tableIndex = (TableIndex) hIndexRowType.index();
        IndexToHKey ih = tableIndex.indexToHKey();
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(1, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // (ix, hx) index row:
        //     declared: I.ix  H.hx
        //     leafward hkey: H.cid
        //     rootward hkey: I.cid
        GroupIndex groupIndex = (GroupIndex) ihLeftIndexRowType.index();
        // H hkey: [C, H.cid, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(2, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, I.cid, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, I.cid, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, I.cid]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        // right join version of (ix, hx) is the same
        groupIndex = (GroupIndex) ihRightIndexRowType.index();
        // H hkey: [C, H.cid, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(2, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, I.cid, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, I.cid, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, I.cid]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        // (ox, ix, hx) index row:
        //     declared: O.ox  I.ix  H.hx
        //     leafward hkey: H.cid
        //     rootward hkey: O.cid  I.cid
        groupIndex = (GroupIndex) ohLeftIndexRowType.index();
        // H hkey: [C, H.cid, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, I.cid, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(5, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, O.cid, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, I.cid]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        // right join version of (ox, ix, hx) is the same
        groupIndex = (GroupIndex) ohRightIndexRowType.index();
        // H hkey: [C, H.cid, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(3, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, I.cid, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(5, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, O.cid, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, I.cid]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        // (cx, ox, ix, hx) index row:
        //     declared: C.cx  O.ox  I.ix  H.hx
        //     leafward hkey: H.cid
        //     rootward hkey: C.cid  O.cid  I.cid
        groupIndex = (GroupIndex) chLeftIndexRowType.index();
        // H hkey: [C, H.cid, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, I.cid, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(7, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, O.cid, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(6, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, C.cid]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(5, ih.getIndexRowPosition(1));
        // right join versino of (cx, ox, ix, hx) is the same
        groupIndex = (GroupIndex) chRightIndexRowType.index();
        // H hkey: [C, H.cid, O, I, H]
        ih = groupIndex.indexToHKey(3);
        assertEquals(5, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(4, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        assertEquals(hOrdinal, ih.getOrdinal(4));
        // I hkey: [C, I.cid, O, I]
        ih = groupIndex.indexToHKey(2);
        assertEquals(4, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(7, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        assertEquals(iOrdinal, ih.getOrdinal(3));
        // O hkey: [C, O.cid, O]
        ih = groupIndex.indexToHKey(1);
        assertEquals(3, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(6, ih.getIndexRowPosition(1));
        assertEquals(oOrdinal, ih.getOrdinal(2));
        // C hkey: [C, C.cid]
        ih = groupIndex.indexToHKey(0);
        assertEquals(2, ih.getLength());
        assertEquals(cOrdinal, ih.getOrdinal(0));
        assertEquals(5, ih.getIndexRowPosition(1));
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
                API.LookupOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[] {
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
        compareRows(expected, cursor(plan, queryContext));
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
                    IndexScanSelector.leftJoinAfter(ihLeftIndexRowType.index(), iRowType.userTable())),
                group,
                ihLeftIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.LookupOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[] {
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
        compareRows(expected, cursor(plan, queryContext));
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
                    IndexScanSelector.rightJoinUntil(ihRightIndexRowType.index(), hRowType.userTable())),
                group,
                ihRightIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.LookupOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[] {
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
        compareRows(expected, cursor(plan, queryContext));
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
                    IndexScanSelector.leftJoinAfter(ohLeftIndexRowType.index(), oRowType.userTable())),
                group,
                ohLeftIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.LookupOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[] {
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
        compareRows(expected, cursor(plan, queryContext));
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
                    IndexScanSelector.rightJoinUntil(ohRightIndexRowType.index(), hRowType.userTable())),
                group,
                ohRightIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.LookupOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[] {
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
        compareRows(expected, cursor(plan, queryContext));
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
                    IndexScanSelector.leftJoinAfter(chLeftIndexRowType.index(), cRowType.userTable())),
                group,
                chLeftIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.LookupOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[] {
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
        compareRows(expected, cursor(plan, queryContext));
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
                    IndexScanSelector.rightJoinUntil(chRightIndexRowType.index(), hRowType.userTable())),
                group,
                chRightIndexRowType,
                Arrays.asList(cRowType, oRowType, iRowType, hRowType),
                API.LookupOption.DISCARD_INPUT);
        RowBase[] expected = new RowBase[] {
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
        compareRows(expected, cursor(plan, queryContext));
    }

    private int[] intArray(int ... ints)
    {
        return ints;
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
    private UserTableRowType cRowType;
    private UserTableRowType oRowType;
    private UserTableRowType iRowType;
    private UserTableRowType hRowType;
    private IndexRowType hIndexRowType;
    private IndexRowType ihLeftIndexRowType;
    private IndexRowType ihRightIndexRowType;
    private IndexRowType ohLeftIndexRowType;
    private IndexRowType ohRightIndexRowType;
    private IndexRowType chLeftIndexRowType;
    private IndexRowType chRightIndexRowType;
    private GroupTable group;
    private int cOrdinal;
    private int oOrdinal;
    private int iOrdinal;
    private int hOrdinal;
}
