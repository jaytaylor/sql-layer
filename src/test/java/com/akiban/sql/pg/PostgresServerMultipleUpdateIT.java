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

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import com.akiban.sql.TestBase;

import java.sql.Statement;
import java.sql.SQLException;

import java.io.File;
import java.util.Collection;

@RunWith(Parameterized.class)
public class PostgresServerMultipleUpdateIT extends PostgresServerITBase
{
    public static final File RESOURCE_DIR = 
        new File(PostgresServerITBase.RESOURCE_DIR, "multiple-update");

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    @Parameters
    public static Collection<Object[]> queries() throws Exception {
        return TestBase.sqlAndExpected(RESOURCE_DIR);
    }

    public PostgresServerMultipleUpdateIT(String caseName, String sql, String expected) {
        super(caseName, sql, expected, null);
    }

    @Test
    public void testMultipleUpdate() throws Exception {
        Statement stmt = connection.createStatement();
        for (String sqls : sql.split("\\;\\s*")) {
            stmt.executeUpdate(sqls);
        }
        stmt.close();
        assertEquals("Difference in " + caseName, expected, dumpData());
    }

}
