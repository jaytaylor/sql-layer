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

package com.akiban.sql.pg;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PostgresServerInstrumentedIT extends PostgresServerSelectIT {

    @Before
    public void enableInstrumentation() throws Exception {
        serviceManager().getPostgresService().getServer().enableInstrumentation();
    }
    
    @After
    public void disableInstrumentation() throws Exception {
        serviceManager().getPostgresService().getServer().disableInstrumentation();
    }
    
    public PostgresServerInstrumentedIT(String caseName, 
                                        String sql, 
                                        String expected, 
                                        String error,
                                        String[] params) {
        super(caseName, sql, expected, error, params);
    }

}
