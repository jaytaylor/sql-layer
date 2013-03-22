
package com.akiban.sql.optimizer;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;
import org.junit.Before;

import java.io.File;

public abstract class DistinctEliminatorTestBase extends OptimizerTestBase {

    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "eliminate-distincts");

    @Before
    public final void loadDDL() throws Exception {
        loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
        ((BoundNodeToString)unparser).setUseBindings(true);
    }
    
    protected String optimized() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        stmt = distinctEliminator.eliminate((DMLStatementNode)stmt);
        return unparser.toString(stmt);
    }

    public DistinctEliminatorTestBase(String caseName, String sql,
                                      String expected, String error) {
        super(caseName, sql, expected, error);
    }
}
