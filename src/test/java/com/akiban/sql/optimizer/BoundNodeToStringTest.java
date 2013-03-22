
package com.akiban.sql.optimizer;

import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.sql.NamedParamsTestBase;
import com.akiban.sql.TestBase;

import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;

import com.akiban.ais.model.AkibanInformationSchema;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static junit.framework.Assert.*;

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
