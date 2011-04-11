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

package com.akiban.server.test.it.bugs.bug705063;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.test.ApiTestBase;
import org.junit.Test;

public final class BadTableStatRequestIT extends ApiTestBase {
    @Test(expected= NoSuchTableException.class)
    public void noTablesDefined() throws InvalidOperationException {
        dml().getTableStatistics(session(), 1, false);
    }

    @Test(expected= NoSuchTableException.class)
    public void wrongTableIdDefined() throws InvalidOperationException {
        int created = createATable();

        dml().getTableStatistics(session(), created + 31, false);
    }

    private int createATable() {
        try {
            return createTable("schema1", "test1", "id int key");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
    }
}
