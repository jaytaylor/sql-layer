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

package com.akiban.server.service.tree;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.operator.ArrayBindings;
import com.akiban.qp.operator.Bindings;
import com.akiban.server.service.session.Session;
import com.akiban.server.test.it.ITBase;
import com.akiban.sql.server.ServerServiceRequirements;

public class PersistitCLILoadablePlanIT extends ITBase {

    @Test
    public void invokePersistitOperation() throws Exception {
        final TreeService treeService = serviceManager().getTreeService();
        final ServerServiceRequirements reqs = new ServerServiceRequirements(
                null, null, null, null, treeService, null, null, null);
        PersistitCLILoadablePlan loadablePlan = new PersistitCLILoadablePlan();
        loadablePlan.setServerServiceRequirements(reqs);
        DirectObjectPlan plan = loadablePlan.plan();
        Session session = serviceManager().getSessionService().createSession();
        
        DirectObjectCursor cursor = plan.cursor(session);
        Bindings bindings = new ArrayBindings(4);
        bindings.set(0, "stat");
        bindings.set(1, "count=3");
        bindings.set(2, "delay=2");
        bindings.set(3, "-a");
        
        int populatedResults = 0;
        int emptyResults = 0;
        
        cursor.open(bindings);
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
        assertEquals(3, populatedResults);
        assertTrue(emptyResults > 0 && emptyResults < 60);
    }
}
