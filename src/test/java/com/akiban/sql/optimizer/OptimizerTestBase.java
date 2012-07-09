/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer;

import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.sql.compiler.ASTTransformTestBase;
import com.akiban.sql.compiler.BooleanNormalizer;
import com.akiban.sql.compiler.TypeComputer;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.service.functions.FunctionsRegistryImpl;

import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.util.Collections;
import java.util.List;

@Ignore
public class OptimizerTestBase extends ASTTransformTestBase
{
    protected OptimizerTestBase(String caseName, String sql, 
                                String expected, String error) {
        super(caseName, sql, expected, error);
    }

    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + OptimizerTestBase.class.getPackage().getName().replace('.', '/'));
    public static final String DEFAULT_SCHEMA = "test";

    // Base class has all possible transformers for convenience.
    protected AISBinder binder;
    protected TypeComputer typeComputer;
    protected BooleanNormalizer booleanNormalizer;
    protected SubqueryFlattener subqueryFlattener;
    protected DistinctEliminator distinctEliminator;

    @Before
    public void makeTransformers() throws Exception {
        parser = new SQLParser();
        parser.setNodeFactory(new BindingNodeFactory(parser.getNodeFactory()));
        unparser = new BoundNodeToString();
        typeComputer = new FunctionsTypeComputer(new FunctionsRegistryImpl());
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        distinctEliminator = new DistinctEliminator(parser);
    }

    public static AkibanInformationSchema parseSchema(List<File> ddls) throws Exception {
        StringBuilder ddlBuilder = new StringBuilder();
        for (File ddl : ddls)
            ddlBuilder.append(fileContents(ddl));
        String sql = ddlBuilder.toString();
        SchemaFactory schemaFactory = new SchemaFactory(DEFAULT_SCHEMA);
        return schemaFactory.ais(sql);
    }

    public static AkibanInformationSchema parseSchema(File ddl) throws Exception {
        return parseSchema(Collections.singletonList(ddl));
    }

    protected AkibanInformationSchema loadSchema(List<File> ddl) throws Exception {
        AkibanInformationSchema ais = parseSchema(ddl);
        binder = new AISBinder(ais, DEFAULT_SCHEMA);
        if (!ais.getViews().isEmpty())
            new TestBinderContext(parser, binder, typeComputer);
        return ais;
    }

    protected AkibanInformationSchema loadSchema(File ddl) throws Exception {
        return loadSchema(Collections.singletonList(ddl));
    }

    protected static class TestBinderContext extends AISBinderContext {
        public TestBinderContext(SQLParser parser, AISBinder binder, TypeComputer typeComputer) {
            this.parser = parser;
            this.defaultSchemaName = DEFAULT_SCHEMA;
            setBinderAndTypeComputer(binder, typeComputer);
        }
    }

}
