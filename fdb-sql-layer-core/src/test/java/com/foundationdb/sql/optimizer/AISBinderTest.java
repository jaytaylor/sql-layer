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

package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

@RunWith(NamedParameterizedRunner.class)
public class AISBinderTest extends OptimizerTestBase 
                           implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "binding");

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        return NamedParamsTestBase.namedCases(sqlAndExpected(RESOURCE_DIR));
    }

    public AISBinderTest(String caseName, String sql, String expected, String error) {
        super(caseName, sql, expected, error);
    }

    @Test
    public void testBinding() throws Exception {
        loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
        File propFile = new File(RESOURCE_DIR, caseName + ".properties");
        if (propFile.exists()) {
            Properties properties = new Properties();
            try (FileInputStream str = new FileInputStream(propFile)) {
                properties.load(str);
            }
            for (String prop : properties.stringPropertyNames()) {
                if ("allowSubqueryMultipleColumns".equals(prop)) {
                    binder.setAllowSubqueryMultipleColumns(Boolean.parseBoolean(properties.getProperty(prop)));
                }
                else if ("resultColumnsAvailableBroadly".equals(prop)) {
                    binder.setResultColumnsAvailableBroadly(Boolean.parseBoolean(properties.getProperty(prop)));
                }
                else {
                    throw new Exception("Unknown binding property: " + prop);
                }
            }
        }
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        return getTree(stmt);
    }
    
    @Override
    public void checkResult(String result) throws IOException {
        assertEqualsWithoutHashes(caseName,
                expected.replaceAll(":\\s+\n",":\n"),
                result.replaceAll(":\\s+\n",":\n"));
    }

}
