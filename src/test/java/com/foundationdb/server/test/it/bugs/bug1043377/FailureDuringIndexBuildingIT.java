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

package com.foundationdb.server.test.it.bugs.bug1043377;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public final class FailureDuringIndexBuildingIT extends ITBase implements TableListener {
    private static final AssertionError EXPECTED_EXCEPTION = new AssertionError();

    @Before
    public void registerListener() {
        serviceManager().getServiceByClass(ListenerService.class).registerTableListener(this);
    }

    @After
    public void deregisterListener() {
        serviceManager().getServiceByClass(ListenerService.class).deregisterTableListener(this);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void injectedFailure() throws Throwable {
        final String SCHEMA = "test";
        final String TABLE = "t1";
        final String INDEX = "lat_lon";
        int tid = createTable(SCHEMA, TABLE, "userID int not null primary key, lat decimal(11,7), lon decimal(11,7)");
        writeRows(
                createNewRow(tid, 1L, "20.5", "11.0"),
                createNewRow(tid, 2L, "90.0", "90.0"),
                createNewRow(tid, 3L, "60.2", "5.34")
        );

        try {
            createIndex(SCHEMA, TABLE, INDEX, "lat", "lon");
            fail("Expected exception");
        } catch(Throwable t) {
            if(t != EXPECTED_EXCEPTION) {
                throw t;
            }
        }

        Table table = getTable(SCHEMA, TABLE);
        assertNull("Index should not be present", table.getIndex(INDEX));
    }


    //
    // TableListener
    //

    @Override
    public void onCreate(Session session, Table table) {
    }

    @Override
    public void onDrop(Session session, Table table) {
    }

    @Override
    public void onTruncate(Session session, Table table, boolean isFast) {
    }

    @Override
    public void onCreateIndex(Session session, Collection<? extends Index> indexes) {
        throw EXPECTED_EXCEPTION;
    }

    @Override
    public void onDropIndex(Session session, Collection<? extends Index> indexes) {
    }
}
