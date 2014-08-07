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

import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.ais.model.AkibanInformationSchema;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public class BoundNodeToStringTest extends NamedParamsTestBase
                                   implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR =
        new File(OptimizerTestBase.RESOURCE_DIR, "unparser");

    protected SQLParser parser;
    protected BoundNodeToString unparser;
    protected AISBinder binder;

    @Before
    public void before() throws Exception {
        parser = new SQLParser();
        unparser = new BoundNodeToString();
        unparser.setUseBindings(true);

        String sql = fileContents(new File(RESOURCE_DIR, "schema.ddl"));
        SchemaFactory schemaFactory = new SchemaFactory(OptimizerTestBase.DEFAULT_SCHEMA);
        AkibanInformationSchema ais = schemaFactory.ais(sql);
        binder = new AISBinder(ais, OptimizerTestBase.DEFAULT_SCHEMA);
    }

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        return namedCases(sqlAndExpected(RESOURCE_DIR));
    }

    public BoundNodeToStringTest(String caseName, String sql, 
                                 String expected, String error) {
        super(caseName, sql, expected, error);
    }

    @Test
    public void testBound() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        return unparser.toString(stmt);
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEquals(caseName, expected, result);
    }

}
