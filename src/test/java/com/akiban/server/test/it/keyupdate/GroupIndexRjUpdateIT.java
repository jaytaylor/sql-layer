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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.Index;
import com.akiban.server.api.dml.scan.NewRow;
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
                "Bergy, 01-01-2001, 1, 10 => " + depthOf(o)
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
                "null, 01-01-2001, 1, 10 => " + depthOf(o)
        );
        writeAndCheck(
                r2 = createNewRow(c, 1L, "Bergy"),
                "Bergy, 01-01-2001, 1, 10 => " + depthOf(o)
        );
        deleteAndCheck(
                r2,
                "null, 01-01-2001, 1, 10 => " + depthOf(o)
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
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i)
        );
        writeAndCheck(
                createNewRow(i, 102L, 11L, 2222),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + depthOf(i)
        );
        writeAndCheck(
                createNewRow(i, 103L, 11L, 3333),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + depthOf(i)
        );
        writeAndCheck(
                createNewRow(o, 12L, 1L, "02-02-2002"),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + depthOf(i)
        );

        writeAndCheck(createNewRow(a, 10001L, 1L, "Causeway"),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + depthOf(i)
        );


        // update parent
        updateAndCheck(
                createNewRow(o, 11L, 1L, "01-01-2001"),
                createNewRow(o, 11L, 1L, "01-01-1999"), // party!
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-1999, 2222, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + depthOf(i)
        );
        // update child
        updateAndCheck(
                createNewRow(i, 102L, 11L, 2222),
                createNewRow(i, 102L, 11L, 2442),
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + depthOf(i),
                "Horton, 01-01-1999, 2442, 1, 11, 102 => " + depthOf(i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + depthOf(i)
        );

        // delete order
        deleteAndCheck(
                createNewRow(o, 11L, 1L, "01-01-1999"),
                "Horton, null, 1111, null, 11, 101 => " + depthOf(i),
                "Horton, null, 3333, null, 11, 103 => " + depthOf(i)
        );
        // delete item
        deleteAndCheck(
                createNewRow(i, 102L, 11L, 222211)
        );
    }

    public GroupIndexRjUpdateIT() {
        super(Index.JoinType.RIGHT);
    }
}
