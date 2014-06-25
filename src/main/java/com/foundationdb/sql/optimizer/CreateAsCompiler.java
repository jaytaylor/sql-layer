package com.foundationdb.sql.optimizer;

import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.sql.IncomparableException;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.embedded.EmbeddedOperatorCompiler;
import com.foundationdb.sql.embedded.JDBCConnection;
import com.foundationdb.sql.optimizer.plan.AST;
import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.server.ServerSession;

import java.util.List;


public class CreateAsCompiler extends EmbeddedOperatorCompiler {

    EmbeddedOperatorCompiler comp;

    public CreateAsCompiler(){
    }
    public EmbeddedOperatorCompiler buildCompiler(ServerSession server, StoreAdapter adapter) {
        //initRules();
        return EmbeddedOperatorCompiler.create((JDBCConnection) server, adapter);
    }
        /** Compile a statement into an operator tree. */
    public BasePlannable makeBasePlan(DMLStatementNode stmt) {
        return makeBasePlan(stmt, new PlanContext(this));
    }

    public BasePlannable makeBasePlan(DMLStatementNode stmt, PlanContext plan) {
        stmt = bindAndTransform(stmt); // Get into standard form.
        plan.setPlan(new AST(stmt, null));
        applyRules(plan);
        return (BasePlannable)plan.getPlan();
    }
    /*@Override
    protected void initRules(List<? extends BaseRule> rules) {
        this.rules = rules;
    }*/
//TODO make rules for changing tree

    /** Apply AST-level transformations before rules. */
    protected DMLStatementNode bindAndTransform(DMLStatementNode stmt)  {
        try {
            binder.bind(stmt);
            stmt = (DMLStatementNode)booleanNormalizer.normalize(stmt);
            try
            {
                typeComputer.compute(stmt);

            }
            catch (IncomparableException e) // catch this and let the resolvers decide
            {
            }

            stmt = subqueryFlattener.flatten(stmt);
            // TODO: Temporary for safety.
            if (Boolean.parseBoolean(getProperty("eliminate-distincts", "true")))
                stmt = distinctEliminator.eliminate(stmt);
            return stmt;
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
    }

}

