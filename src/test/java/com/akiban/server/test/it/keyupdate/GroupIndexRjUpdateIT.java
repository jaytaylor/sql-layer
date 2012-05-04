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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.Index;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Ignore;
import org.junit.Test;

public final class GroupIndexRjUpdateIT extends GIUpdateITBase {

    @Test
    public void placeholderNoOrphan() {
        final NewRow r1, r2;
        groupIndex("c.name, o.when");
        writeAndCheck(
                r1 = createNewRow(c, 1L, "Bergy")
        );
        writeAndCheck(
                r2 = createNewRow(o, 10L, 1L, "01-01-2001"),
                "Bergy, 01-01-2001, 1, 10, 1 => " + containing(c, o)
        );
        deleteAndCheck(r2);
        deleteAndCheck(r1);
    }

    @Test
    public void placeholderWithOrphan() {
        final NewRow r1, r2;
        groupIndex("c.name, o.when");
        writeAndCheck(
                r1 = createNewRow(o, 10L, 1L, "01-01-2001"),
                "null, 01-01-2001, 1, 10, null => " + containing(o)
        );
        writeAndCheck(
                r2 = createNewRow(c, 1L, "Bergy"),
                "Bergy, 01-01-2001, 1, 10, 1 => " + containing(c, o)
        );
        deleteAndCheck(
                r2,
                "null, 01-01-2001, 1, 10, null => " + containing(o)
        );
        deleteAndCheck(r1);
    }

    @Test
    public void coiNoOrphan() {
        groupIndex("c.name, o.when, i.sku");

        writeAndCheck(
                createNewRow(c, 1L, "Horton")
        );
        writeAndCheck(
                createNewRow(o, 11L, 1L, "01-01-2001")
        );
        writeAndCheck(
                createNewRow(i, 101L, 11L, 1111),
                "Horton, 01-01-2001, 1111, 1, 11, 101, 1, 11 => " + containing(c, o, i)
        );
        writeAndCheck(
                createNewRow(i, 102L, 11L, 2222),
                "Horton, 01-01-2001, 1111, 1, 11, 101, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102, 1, 11 => " + containing(c, o, i)
        );
        writeAndCheck(
                createNewRow(i, 103L, 11L, 3333),
                "Horton, 01-01-2001, 1111, 1, 11, 101, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103, 1, 11 => " + containing(c, o, i)
        );
        writeAndCheck(
                createNewRow(o, 12L, 1L, "02-02-2002"),
                "Horton, 01-01-2001, 1111, 1, 11, 101, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103, 1, 11 => " + containing(c, o, i)
        );

        writeAndCheck(createNewRow(a, 10001L, 1L, "Causeway"),
                "Horton, 01-01-2001, 1111, 1, 11, 101, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103, 1, 11 => " + containing(c, o, i)
        );


        // update parent
        updateAndCheck(
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(o, 11L, 1L, "01-01-1999"), // party!
                "Horton, 01-01-1999, 1111, 1, 11, 101, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-1999, 2222, 1, 11, 102, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103, 1, 11 => " + containing(c, o, i)
        );
        // update child
        updateAndCheck(
                createNewRow(i, 102L, 11L, 2222),
                createNewRow(i, 102L, 11L, 2442),
                "Horton, 01-01-1999, 1111, 1, 11, 101, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-1999, 2442, 1, 11, 102, 1, 11 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103, 1, 11 => " + containing(c, o, i)
        );

        // delete order
        deleteAndCheck(
                createNewRow(o, 11L, 1L, "01-01-1999"),
                "null, null, 1111, null, 11, 101, null, null => " + containing(i),
                "null, null, 2442, null, 11, 102, null, null => " + containing(i),
                "null, null, 3333, null, 11, 103, null, null => " + containing(i)
        );
        // delete item
        deleteAndCheck(
                createNewRow(i, 102L, 11L, 222211),
                "null, null, 1111, null, 11, 101, null, null => " + containing(i),
                "null, null, 3333, null, 11, 103, null, null => " + containing(i)
        );
    }

    @Test
    public void createGIOnFullyPopulatedTables() {
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111)
        );
        groupIndex("name_when_sku", "c.name, o.when, i.sku");
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101, 1, 11 => " + containing("name_when_sku", c, o, i)
        );
    }

    @Test
    public void createGIOnPartiallyPopulatedTablesFromLeaf() {
        writeRows(
                createNewRow(i, 101L, 11L, 1111)
        );
        groupIndex("name_when_sku", "c.name, o.when, i.sku");
        checkIndex("name_when_sku",
                "null, null, 1111, null, 11, 101, null, null => " + containing("name_when_sku", i)
        );
    }

    @Test
    public void createGiOnPartiallyPopulatedTablesFromMiddle() {
        writeRows(
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111)
        );
        groupIndex("when_sku","o.when, i.sku");
        checkIndex("when_sku",
                "01-01-2001, 1111, 1, 11, 101, 11 => " + containing("when_sku", o, i)
        );
    }

    @Test
    public void ihIndexNoOrphans() {
        String indexName = groupIndex("i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001, 101 => " + containing(i, h));

        // delete from root on up
        dml().deleteRow(session(), createNewRow(c, 1L, "Horton"));
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001, 101 => " + containing(i, h));

        dml().deleteRow(session(), createNewRow(o, 11L, 1L, "01-01-2001 => " + containing(i, h)));
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001, 101 => " + containing(i, h));

        dml().deleteRow(session(), createNewRow(i, 101L, 11L, 1111));
        checkIndex(indexName, "null, handle with care, null, null, 101, 1001, null => " + containing(h));

        dml().deleteRow(session(), createNewRow(h, 1001L, 101L, "handle with care"));
        checkIndex(indexName);
    }

    @Test
    public void adoptionChangesHKeyNoCustomer() {
        String indexName = groupIndex("i.sku, h.handling_instructions");
        writeRows(
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001, 101 => " + containing(i, h)
        );

        // bring an o that adopts the i
        final NewRow oRow;
        writeAndCheck(
                oRow = createNewRow(o, 11L, 1L, "01-01-2001"),
                "1111, handle with care, 1, 11, 101, 1001, 101 => " + containing(i, h)
        );
        deleteAndCheck(
                oRow,
                "1111, handle with care, null, 11, 101, 1001, 101 => " + containing(i, h)
        );
    }

    @Test
    public void adoptionChangesHKeyWithC() {
        String indexName = groupIndex("i.sku, h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001, 101 => " + containing(i, h)
        );

        // bring an o that adopts the i
        dml().writeRow(session(), createNewRow(o, 11L, 1L, "01-01-2001"));
        checkIndex(indexName,
                "1111, handle with care, 1, 11, 101, 1001, 101 => " + containing(i, h)
        );
    }
    @Test
    public void updateModifiesHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        groupIndex("i.sku, h.handling_instructions");
        writeAndCheck(createNewRow(c, 1L, "Horton"));

        writeAndCheck(createNewRow(o, 11L, 1L, "01-01-2001"));

        writeAndCheck(createNewRow(i, 101L, 11L, "1111"));

        writeAndCheck(
                createNewRow(h, 1001L, 101L, "don't break"),
                "1111, don't break, 1, 11, 101, 1001, 101 => " + containing(i, h)
        );

        writeAndCheck(
                createNewRow(c, 2L, "David"),
                "1111, don't break, 1, 11, 101, 1001, 101 => " + containing(i, h)
        );

        writeAndCheck(
                createNewRow(o, 12L, 2L, "02-02-2002"),
                "1111, don't break, 1, 11, 101, 1001, 101 => " + containing(i, h)
        );

        writeAndCheck(
                createNewRow(h, 1002L, 102L, "do break"),
                "null, do break, null, null, 102, 1002, null => " + containing(h),
                "1111, don't break, 1, 11, 101, 1001, 101 => " + containing(i, h)
        );

        updateAndCheck(
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(i, 102L, 12L, "2222"),
                "null, don't break, null, null, 101, 1001, null => " + containing(h),
                "2222, do break, 2, 12, 102, 1002, 102 => " + containing(i, h)
        );
    }

    public GroupIndexRjUpdateIT() {
        super(Index.JoinType.RIGHT);
    }
}
