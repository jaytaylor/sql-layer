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

package com.akiban.server.test.it.pstraverse;

import org.junit.Test;

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
    @Test(expected=IllegalArgumentException.class)
    public void traverseOrdersPK() throws Exception {
        super.traverseOrdersPK();
    }

    @Override
    @Test(expected=IllegalArgumentException.class)
    public void traverseItemsPK() throws Exception {
        super.traverseItemsPK();
    }
}
