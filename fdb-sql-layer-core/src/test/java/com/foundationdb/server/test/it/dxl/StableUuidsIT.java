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

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public final class StableUuidsIT extends ITBase {
    @Test
    public void uuidsSurviveRestart() throws Exception {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");
        UUID origUuid = ais().getTable(tableId).getUuid();
        assertNotNull("original UUID is null", origUuid);
        safeRestartTestServices();
        UUID afterRestartUuid = ais().getTable(tableId).getUuid();
        assertNotNull("original UUID is null", afterRestartUuid);
        assertEquals("UUIDs for customer", origUuid, afterRestartUuid);
        assertNotSame("UUIDs are same object", origUuid, afterRestartUuid);
    }
}
