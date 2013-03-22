
package com.akiban.server.test.it.dxl;

import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public final class StableUuidsIT extends ITBase {
    @Test
    public void uuidsSurviveRestart() throws Exception {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");
        UUID origUuid = ais().getUserTable(tableId).getUuid();
        assertNotNull("original UUID is null", origUuid);
        safeRestartTestServices();
        UUID afterRestartUuid = ais().getUserTable(tableId).getUuid();
        assertNotNull("original UUID is null", afterRestartUuid);
        assertEquals("UUIDs for customer", origUuid, afterRestartUuid);
        assertNotSame("UUIDs are same object", origUuid, afterRestartUuid);
    }
}
