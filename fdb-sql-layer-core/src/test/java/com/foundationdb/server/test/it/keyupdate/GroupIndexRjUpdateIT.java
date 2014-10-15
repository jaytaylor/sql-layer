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
import com.foundationdb.server.api.dml.scan.NewRow;
import org.junit.Test;

public final class GroupIndexRjUpdateIT extends GIUpdateITBase {

    @Test
    public void placeholderNoOrphan() {
        final NewRow r1, r2;
        groupIndex("c.name", "o.when");
        writeAndCheck(
                r1 = createNewRow(c, 1L, "Bergy")
        );
        writeAndCheck(
                r2 = createNewRow(o, 10L, 1L, "01-01-2001"),
                "Bergy, 01-01-2001, 1, 10 => " + containing(c, o)
        );
        deleteAndCheck(r2);
        deleteAndCheck(r1);
    }

    @Test
    public void placeholderWithOrphan() {
        final NewRow r1, r2;
        groupIndex("c.name", "o.when");
        writeAndCheck(
                r1 = createNewRow(o, 10L, 1L, "01-01-2001"),
                "null, 01-01-2001, 1, 10 => " + containing(o)
        );
        writeAndCheck(
                r2 = createNewRow(c, 1L, "Bergy"),
                "Bergy, 01-01-2001, 1, 10 => " + containing(c, o)
        );
        deleteAndCheck(
                r2,
                "null, 01-01-2001, 1, 10 => " + containing(o)
        );
        deleteAndCheck(r1);
    }

    @Test
    public void coiNoOrphan() {
        groupIndex("c.name", "o.when", "i.sku");

        writeAndCheck(
                createNewRow(c, 1L, "Horton")
        );
        writeAndCheck(
                createNewRow(o, 11L, 1L, "01-01-2001")
        );
        writeAndCheck(
                createNewRow(i, 101L, 11L, 1111),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i)
        );
        writeAndCheck(
                createNewRow(i, 102L, 11L, 2222),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i)
        );
        writeAndCheck(
                createNewRow(i, 103L, 11L, 3333),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i)
        );
        writeAndCheck(
                createNewRow(o, 12L, 1L, "02-02-2002"),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i)
        );

        writeAndCheck(createNewRow(a, 10001L, 1L, "Causeway"),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i)
        );


        // update parent
        updateAndCheck(
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(o, 11L, 1L, "01-01-1999"), // party!
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-1999, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + containing(c, o, i)
        );
        // update child
        updateAndCheck(
                createNewRow(i, 102L, 11L, 2222),
                createNewRow(i, 102L, 11L, 2442),
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-1999, 2442, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + containing(c, o, i)
        );

        // delete order
        deleteAndCheck(
                createNewRow(o, 11L, 1L, "01-01-1999"),
                "null, null, 1111, null, 11, 101 => " + containing(i),
                "null, null, 2442, null, 11, 102 => " + containing(i),
                "null, null, 3333, null, 11, 103 => " + containing(i)
        );
        // delete item
        deleteAndCheck(
                createNewRow(i, 102L, 11L, 222211),
                "null, null, 1111, null, 11, 101 => " + containing(i),
                "null, null, 3333, null, 11, 103 => " + containing(i)
        );
    }

    @Test
    public void createGIOnFullyPopulatedTables() {
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111)
        );
        groupIndexNamed("name_when_sku", "c.name", "o.when", "i.sku");
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing("name_when_sku", c, o, i)
        );
    }

    @Test
    public void createGIOnPartiallyPopulatedTablesFromLeaf() {
        writeRows(
                createNewRow(i, 101L, 11L, 1111)
        );
        groupIndexNamed("name_when_sku", "c.name", "o.when", "i.sku");
        checkIndex("name_when_sku",
                "null, null, 1111, null, 11, 101 => " + containing("name_when_sku", i)
        );
    }

    @Test
    public void createGiOnPartiallyPopulatedTablesFromMiddle() {
        writeRows(
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111)
        );
        groupIndexNamed("when_sku", "o.when", "i.sku");
        checkIndex("when_sku",
                "01-01-2001, 1111, 1, 11, 101 => " + containing("when_sku", o, i)
        );
    }

    @Test
    public void ihIndexNoOrphans() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h));

        // delete from root on up
        dml().deleteRow(session(), createNewRow(c, 1L, "Horton"), false);
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h));

        dml().deleteRow(session(), createNewRow(o, 11L, 1L, "01-01-2001 => " + containing(i, h)), false);
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h));

        dml().deleteRow(session(), createNewRow(i, 101L, 11L, 1111), false);
        checkIndex(indexName, "null, handle with care, null, null, 101, 1001 => " + containing(h));

        dml().deleteRow(session(), createNewRow(h, 1001L, 101L, "handle with care"), false);
        checkIndex(indexName);
    }

    @Test
    public void adoptionChangesHKeyNoCustomer() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h)
        );

        // bring an o that adopts the i
        final NewRow oRow;
        writeAndCheck(
                oRow = createNewRow(o, 11L, 1L, "01-01-2001"),
                "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h)
        );
        deleteAndCheck(
                oRow,
                "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h)
        );
    }

    @Test
    public void adoptionChangesHKeyWithC() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                createNewRow(c, 1L, "Horton"),
                createNewRow(i, 101L, 11L, 1111),
                createNewRow(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h)
        );

        // bring an o that adopts the i
        dml().writeRow(session(), createNewRow(o, 11L, 1L, "01-01-2001"));
        checkIndex(indexName,
                "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h)
        );
    }
    @Test
    public void updateModifiesHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        groupIndex("i.sku", "h.handling_instructions");
        writeAndCheck(createNewRow(c, 1L, "Horton"));

        writeAndCheck(createNewRow(o, 11L, 1L, "01-01-2001"));

        writeAndCheck(createNewRow(i, 101L, 11L, "1111"));

        writeAndCheck(
                createNewRow(h, 1001L, 101L, "don't break"),
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h)
        );

        writeAndCheck(
                createNewRow(c, 2L, "David"),
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h)
        );

        writeAndCheck(
                createNewRow(o, 12L, 2L, "02-02-2002"),
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h)
        );

        writeAndCheck(
                createNewRow(h, 1002L, 102L, "do break"),
                "null, do break, null, null, 102, 1002 => " + containing(h),
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h)
        );

        updateAndCheck(
                createNewRow(i, 101L, 11L, "1111"),
                createNewRow(i, 102L, 12L, "2222"),
                "null, don't break, null, null, 101, 1001 => " + containing(h),
                "2222, do break, 2, 12, 102, 1002 => " + containing(i, h)
        );
    }

    public GroupIndexRjUpdateIT() {
        super(Index.JoinType.RIGHT);
    }
}
