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

package com.foundationdb.qp.loadableplan.std;

import static org.junit.Assert.*;

import java.util.List;

import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.test.it.PersistitITBase;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;

import org.junit.Test;

import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;

public class PersistitCLILoadablePlanIT extends PersistitITBase
{
    @Test
    public void invokePersistitOperation() throws Exception {
        PersistitCLILoadablePlan loadablePlan = new PersistitCLILoadablePlan();
        DirectObjectPlan plan = loadablePlan.plan();

        StoreAdapter adapter = newStoreAdapter();
        QueryContext queryContext = queryContext(adapter);
        QueryBindings queryBindings = queryContext.createBindings();

        DirectObjectCursor cursor = plan.cursor(queryContext, queryBindings);
        queryBindings.setValue(0, new Value(MString.varcharFor("stat"), "stat"));
        queryBindings.setValue(1, new Value(MString.varcharFor("count=3"), "count=3"));
        queryBindings.setValue(2, new Value(MString.varcharFor("delay=2"), "delay=2"));
        queryBindings.setValue(3, new Value(MString.varcharFor("-a"), "-a"));
        
        int populatedResults = 0;
        int emptyResults = 0;
        
        cursor.open();
        while(true) {
            List<? extends Object> columns = cursor.next();
            if (columns == null) {
                break;
            }
            if (columns.isEmpty()) {
                emptyResults++;
            } else {
                assertEquals(1, columns.size());
                assertTrue(columns.get(0) instanceof String);
                populatedResults++;
            }
        }
        cursor.close();
        assertEquals(3, populatedResults);
        assertTrue(emptyResults > 0 && emptyResults < 60);
    }
}
