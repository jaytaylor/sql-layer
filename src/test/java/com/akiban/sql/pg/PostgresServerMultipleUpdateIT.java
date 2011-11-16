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

import com.akiban.sql.NamedParamsTestBase;
import com.akiban.sql.TestBase;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static junit.framework.Assert.*;

import java.sql.Statement;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public class PostgresServerMultipleUpdateIT extends PostgresServerFilesITBase 
                                            implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(PostgresServerITBase.RESOURCE_DIR, "multiple-update");

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        return NamedParamsTestBase.namedCases(TestBase.sqlAndExpected(RESOURCE_DIR));
    }

    public PostgresServerMultipleUpdateIT(String caseName, String sql, 
                                          String expected, String error) {
        super(caseName, sql, expected, error, null);
    }

    @Test
    public void testMultipleUpdate() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        Statement stmt = connection.createStatement();
        for (String sqls : sql.split("\\;\\s*")) {
            stmt.executeUpdate(sqls);
        }
        stmt.close();
        return dumpData();
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEquals(caseName, expected, result);
    }

}
