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

package com.foundationdb.server.test.it.bugs.bug705063;

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchTableIdException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public final class BadTableStatRequestIT extends ITBase {
    @Test(expected= NoSuchTableIdException.class)
    public void noTablesDefined() throws InvalidOperationException {
        dml().getTableStatistics(session(), -1, false);
    }

    @Test(expected= NoSuchTableIdException.class)
    public void wrongTableIdDefined() throws InvalidOperationException {
        int created = createATable();

        dml().getTableStatistics(session(), created + 31, false);
    }

    private int createATable() {
        try {
            return createTable("schema1", "test1", "id int not null primary key");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }
    }
}
