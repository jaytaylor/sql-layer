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
import org.junit.Test;

public final class GroupIndexRjUpdateIT extends GIUpdateITBase {

    @Test
    public void placeholderNoOrphan() {
        groupIndex("name_when", "c.name, o.when");
        writeRows(
                createNewRow(o, 10L, 1L, "01-01-2001")
        );
        checkIndex(
                "name_when",
                "null, 01-01-2001, 1, 10 => " + depthOf(o)
        );
        writeRows(
                createNewRow(c, 1L, "Bergy")
        );
        checkIndex(
                "name_when",
                "Bergy, 01-01-2001, 1, 10 => " + depthOf(o)
        );
    }

    public GroupIndexRjUpdateIT() {
        super(Index.JoinType.RIGHT);
    }
}
