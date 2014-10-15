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

package com.foundationdb.server.test.it.pstraverse;

import org.junit.Test;

import java.util.Arrays;

public final class CascadingKeysTraversalIT extends KeysBase {
    @Override
    protected String ordersPK() {
        return "cid,oid";
    }

    @Override
    protected String itemsPK() {
        return "cid,oid,iid";
    }

    @Override
    @Test @SuppressWarnings(value={"unused", "unchecked"}) // junit will invoke
    public void traverseOrdersPK() throws Exception {
        traversePK(
                orders(),
                Arrays.asList(71L, 81L),
                Arrays.asList(72L, 82L)
        );
    }

    @Override
    @Test @SuppressWarnings(value={"unused", "unchecked"}) // junit will invoke
    public void traverseItemsPK() throws Exception {
        traversePK(
                items(),
                Arrays.asList(71L, 81L, 91L),
                Arrays.asList(71L, 81L, 92L),
                Arrays.asList(72L, 82L, 93L)
        );
    }
}
