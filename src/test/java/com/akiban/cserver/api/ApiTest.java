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

package com.akiban.cserver.api;

import static org.junit.Assert.assertEquals;

import com.akiban.ais.model.TableName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.cserver.CServerTestCase;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
public final class ApiTest extends CServerTestCase {

    private class ApiPair {
        final DMLFunctionsImpl dml;
        final DDLFunctionsImpl ddl;
        
        private ApiPair() {
            ddl = new DDLFunctionsImpl();
            dml = new DMLFunctionsImpl(ddl);
        }
    }
    
    @Before
    public void setUp() throws Exception {
        super.baseSetUp();
    }
    
    @After
    public void tearDown() throws Exception {
        super.baseTearDown();
    }

    @Test
    public void testAutoIncrement() throws InvalidOperationException {
        ApiPair apiPair = new ApiPair();
        final Session session = new SessionImpl();
        apiPair.ddl.createTable(session, "sc1", "CREATE TABLE t1(c1 TINYINT   AUTO_INCREMENT NULL KEY ) AUTO_INCREMENT=10");
        final int tableId = apiPair.ddl.getTableId(session, new TableName("sc1", "t1"));
        TableStatistics tableStats = apiPair.dml.getTableStatistics(session, tableId, false);
        assertEquals("autoinc value", 9L, tableStats.getAutoIncrementValue());
    }
}
