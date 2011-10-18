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

    public GroupIndexRjUpdateIT() {
        super(Index.JoinType.RIGHT);
    }
}
