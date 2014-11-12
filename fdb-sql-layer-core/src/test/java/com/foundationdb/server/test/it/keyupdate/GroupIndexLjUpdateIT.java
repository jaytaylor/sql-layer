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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.scan.NewRow;
import org.junit.Test;

public final class GroupIndexLjUpdateIT extends GIUpdateITBase {

    @Test
    public void placeholderNoOrphan() {
        groupIndex("c.name", "o.when");
        writeAndCheck(
                row(c, 1L, "Bergy"),
                "Bergy, null, 1, null => " + containing(c)
        );
        writeAndCheck(
                row(o, 10L, 1L, "01-01-2001"),
                "Bergy, 01-01-2001, 1, 10 => " + containing(c, o)
        );
    }

    @Test
    public void placeholderWithOrphan() {
        groupIndex("c.name", "o.when");
        writeAndCheck(
                row(o, 10L, 1L, "01-01-2001")
        );
        writeAndCheck(
                row(c, 1L, "Bergy"),
                "Bergy, 01-01-2001, 1, 10 => " + containing(c, o)
        );
    }

    @Test
    public void deleteSecondOinCO() {
        groupIndex("c.name", "o.when");
        final Row customer, firstOrder, secondOrder;
        writeAndCheck(
                customer = row(c, 1L, "Joe"),
                "Joe, null, 1, null => " + containing(c)
        );
        writeAndCheck(
                firstOrder = row(o, 11L, 1L, "01-01-01"),
                "Joe, 01-01-01, 1, 11 => " + containing(c, o)
        );
        writeAndCheck(
                secondOrder = row(o, 12L, 1L, "02-02-02"),
                "Joe, 01-01-01, 1, 11 => " + containing(c, o),
                "Joe, 02-02-02, 1, 12 => " + containing(c, o)
        );
        deleteAndCheck(
                secondOrder,
                "Joe, 01-01-01, 1, 11 => " + containing(c, o)
        );
        deleteAndCheck(
                firstOrder,
                "Joe, null, 1, null => " + containing(c)
        );
        deleteAndCheck(
                customer
        );
    }

    @Test
    public void coiGIsNoOrphan() {
        groupIndex("c.name", "o.when", "i.sku");
        // write write write
        writeAndCheck(
                row(c, 1L, "Horton"),
                "Horton, null, null, 1, null, null => " + containing(c)
        );
        writeAndCheck(
                row(o, 11L, 1L, "01-01-2001"),
                "Horton, 01-01-2001, null, 1, 11, null => " + containing(c, o)
        );
        writeAndCheck(
                row(i, 101L, 11L, 1111),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i)
        );
        writeAndCheck(
                row(i, 102L, 11L, 2222),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i)
        );

        writeAndCheck(
                row(i, 103L, 11L, 3333),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i)
        );

        writeAndCheck(
                row(o, 12L, 1L, "02-02-2002"),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i),
                "Horton, 02-02-2002, null, 1, 12, null => " + containing(c, o)
        );

        writeAndCheck(row(a, 10001L, 1L, "Causeway"),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i),
                "Horton, 02-02-2002, null, 1, 12, null => " + containing(c, o)
        );

        // update parent
        updateAndCheck(
                row(o, 11L, 1L, "01-01-2001"),
                row(o, 11L, 1L, "01-01-1999"), // party!
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-1999, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + containing(c, o, i),
                "Horton, 02-02-2002, null, 1, 12, null => " + containing(c, o)
        );
        // update child
        updateAndCheck(
                row(i, 102L, 11L, 2222),
                row(i, 102L, 11L, 2442),
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-1999, 2442, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + containing(c, o, i),
                "Horton, 02-02-2002, null, 1, 12, null => " + containing(c, o)
        );

        // delete child
        deleteAndCheck(
                row(i, 102L, 11L, 222211),
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + containing(c, o, i),
                "Horton, 02-02-2002, null, 1, 12, null => " + containing(c, o)
        );
        // delete parent
        deleteAndCheck(
                row(o, 11L, 1L, "01-01-2001"),
                "Horton, 02-02-2002, null, 1, 12, null => " + containing(c, o)
        );
    }

    @Test
    public void createGIOnFullyPopulatedTables() {
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, 1111)
        );
        groupIndexNamed("name_when_sku", "c.name", "o.when", "i.sku");
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing("name_when_sku", c, o, i)
        );
    }

    @Test
    public void createGIOnPartiallyPopulatedTablesFromRoot() {
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001")
        );
        groupIndexNamed("name_when_sku", "c.name", "o.when", "i.sku");
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, null, 1, 11, null => " + containing("name_when_sku", c, o)
        );
    }
    
    @Test
    public void createGIOnPartiallyPopulatedTablesFromMiddle() {
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001")
        );
        groupIndexNamed("when_sku", "o.when", "i.sku");
        checkIndex("when_sku",
                "01-01-2001, null, 1, 11, null => " + containing("when_sku", o)
        );
    }

    @Test
    public void ihIndexNoOrphans() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h));

        // delete from root on down
        deleteRow(c, 1L, "Horton");
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h));

        deleteRow(o, 11L, 1L, "01-01-2001 => " + containing(i, h));
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h));

        deleteRow(i, 101L, 11L, 1111);
        checkIndex(indexName);

        deleteRow(h, 1001L, 101L, "handle with care");
        checkIndex(indexName);
    }

    @Test
    public void ihIndexOIsOrphaned() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h));

        // delete from root on down

        deleteRow(o, 11L, 1L, "01-01-2001");
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h));

        deleteRow(i, 101L, 11L, 1111);
        checkIndex(indexName);

        deleteRow(h, 1001L, 101L, "handle with care");
        checkIndex(indexName);
    }

    @Test
    public void ihIndexIIsOrphaned() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(c, -1L, "Notroh"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h));

        // delete from root on up

        deleteRow(i, 101L, 11L, 1111);
        checkIndex(indexName);

        deleteRow(h, 1001L, 101L, "handle with care");
        checkIndex(indexName);
    }

    @Test
    public void ihIndexIIsOrphanedButCExists() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h));

        // delete from root on up

        deleteRow(i, 101L, 11L, 1111);
        checkIndex(indexName);

        deleteRow(h, 1001L, 101L, "handle with care");
        checkIndex(indexName);
    }

    @Test
    public void ihIndexHIsOrphaned() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName);

        // delete from root on up

        deleteRow(h, 1001L, 101L, "handle with care");
        checkIndex(indexName);
    }

    @Test
    public void adoptionChangesHKeyNoCustomer() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h)
        );

        // bring an o that adopts the i
        writeRow(o, 11L, 1L, "01-01-2001");
        checkIndex(indexName,
                "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h)
        );
    }

    @Test
    public void adoptionChangesHKeyWithC() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h)
        );

        // bring an o that adopts the i
        writeRow(o, 11L, 1L, "01-01-2001");
        checkIndex(indexName,
                "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h)
        );
    }

    @Test
    public void testTwoBranches() {
        groupIndexNamed("when_name", "o.when", "c.name");
        groupIndexNamed("second_idx", "c.name", "a.street");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(o, 12L, 1L, "03-03-2003"),
                row(a, 21L, 1L, "Harrington"),
                row(a, 22L, 1L, "Causeway"),
                row(c, 2L, "David"),
                row(o, 13L, 2L, "02-02-2002"),
                row(a, 23L, 2L, "Highland")
        );

        checkIndex(
                "when_name",
                "01-01-2001, Horton, 1, 11 => " + containing("when_name", c, o),
                "02-02-2002, David, 2, 13 => " + containing("when_name", c, o),
                "03-03-2003, Horton, 1, 12 => " + containing("when_name", c, o)
        );
        checkIndex(
                "second_idx",
                "David, Highland, 2, 23 => " + containing("second_idx", c, a),
                "Horton, Causeway, 1, 22 => " + containing("second_idx", c, a),
                "Horton, Harrington, 1, 21 => " + containing("second_idx",c, a)
        );
    }

    @Test
    public void updateModifiesHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(row(c, 1L, "Horton"));
        checkIndex(indexName);

        writeRows(row(o, 11L, 1L, "01-01-2001"));
        checkIndex(indexName);

        writeRows(row(i, 101L, 11L, "1111"));
        checkIndex(indexName, "1111, null, 1, 11, 101, null => " + containing(i));

        writeRows(row(h, 1001L, 101L, "don't break"));
        checkIndex(indexName, "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h));

        writeRows(row(c, 2L, "David"));
        checkIndex(indexName, "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h));

        writeRows(row(o, 12L, 2L, "02-02-2002"));
        checkIndex(indexName, "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h));

        writeRows(row(i, 102L, 12L, "2222"));
        checkIndex(
                indexName,
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );

        updateRow(
                row(h, 1001L, 101L, "don't break"),
                row(h, 1001L, 102L, "don't break")
        );

        checkIndex(
                indexName,
                "1111, null, 1, 11, 101, null => " + containing(i),
                "2222, don't break, 2, 12, 102, 1001 => " + containing(i, h)
        );
    }

    @Test
    public void updateModifiesHKeyDirectlyAboveBranch() {
        // branch is I-H, we're modifying the hkey of an I
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, "1111"),
                row(h, 1001L, 101L, "don't break"),
                row(c, 2L, "David"),
                row(o, 12L, 2L, "02-02-2002"),
                row(i, 102L, 12L, "2222")
        );
        checkIndex(
                indexName,
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );

        updateRow(
                row(i, 101L, 11L, "1111"),
                row(i, 101L, 12L, "1111")
        );

        // TODO: This test fails on the first row. last two nulls are 101, 1001, reflecting the old home
        // TODO: of the row. Ancestor lookup broken due to use of wrong column?
        checkIndex(
                indexName,
                "1111, don't break, 2, 12, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );
    }

    @Test
    public void updateModifiesHKeyHigherAboveBranch() {
        // branch is I-H, we're modifying the hkey of an O referenced by an I
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, "1111"),
                row(h, 1001L, 101L, "don't break"),
                row(c, 2L, "David"),
                row(o, 12L, 2L, "02-02-2002"),
                row(i, 102L, 12L, "2222")
        );
        checkIndex(
                indexName,
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );

        updateRow(
                row(o, 11L, 1L, "01-01-2001"),
                row(o, 11L, 2L, "01-01-2001")
        );

        checkIndex(
                indexName,
                "1111, don't break, 2, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );
    }

    @Test
    public void updateOrphansHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, "1111"),
                row(h, 1001L, 101L, "don't break"),
                row(c, 2L, "David"),
                row(o, 12L, 2L, "02-02-2002"),
                row(i, 102L, 12L, "2222")
        );
        checkIndex(
                indexName,
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );

        updateRow(
                row(h, 1001L, 101L, "don't break"),
                row(h, 1001L, 666L, "don't break")
        );

        checkIndex(indexName,
                "1111, null, 1, 11, 101, null => " + containing(i),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );
    }

    @Test
    public void updateMovesHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, "1111"),
                row(h, 1001L, 101L, "don't break"),
                row(c, 2L, "David"),
                row(o, 12L, 2L, "02-02-2002"),
                row(i, 102L, 12L, "2222"),

                row(o, 66L, 6L, "03-03-2003"),
                row(i, 666L, 66L, "6666")
        );
        checkIndex(
                indexName,
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i),
                "6666, null, 6, 66, 666, null => " + containing(i)
        );

        updateRow(
                row(h, 1001L, 101L, "don't break"),
                row(h, 1001L, 666L, "don't break")
        );

        checkIndex(
                indexName,
                "1111, null, 1, 11, 101, null => " + containing(i),
                "2222, null, 2, 12, 102, null => " + containing(i),
                "6666, don't break, 6, 66, 666, 1001 => " + containing(i, h)
        );
    }

    @Test
    public void updateOrphansHKeyDirectlyAboveBranch() {
        // branch is I-H, we're modifying the hkey of an I
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, "1111"),
                row(h, 1001L, 101L, "don't break"),
                row(c, 2L, "David"),
                row(o, 12L, 2L, "02-02-2002"),
                row(i, 102L, 12L, "2222")
        );
        checkIndex(
                indexName,
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );

        updateRow(
                row(i, 101L, 11L, "1111"),
                row(i, 101L, 66L, "1111")
        );

        checkIndex(
                indexName,
                "1111, don't break, null, 66, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );
    }

    @Test
    public void updateOrphansHKeyHigherAboveBranch() {
        // branch is I-H, we're modifying the hkey of an O referenced by an I
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, "1111"),
                row(h, 1001L, 101L, "don't break"),
                row(c, 2L, "David"),
                row(o, 12L, 2L, "02-02-2002"),
                row(i, 102L, 12L, "2222")
        );
        checkIndex(
                indexName,
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );

        updateRow(
                row(o, 11L, 1L, "01-01-2001"),
                row(o, 11L, 6L, "01-01-2001")
        );

        checkIndex(
                indexName,
                "1111, don't break, 6, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );
    }

    /**
     * Create the endgame of {@linkplain #updateOrphansHKeyHigherAboveBranch} initially, as a santy check
     */
    @Test
    public void originallyOrphansHKeyHigherAboveBranch() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 6L, "01-01-2001"),
                row(i, 101L, 11L, "1111"),
                row(h, 1001L, 101L, "don't break"),
                row(c, 2L, "David"),
                row(o, 12L, 2L, "02-02-2002"),
                row(i, 102L, 12L, "2222")
        );
        checkIndex(
                indexName,
                "1111, don't break, 6, 11, 101, 1001 => " + containing(i, h),
                "2222, null, 2, 12, 102, null => " + containing(i)
        );
    }

    public GroupIndexLjUpdateIT() {
        super(Index.JoinType.LEFT);
    }
}
