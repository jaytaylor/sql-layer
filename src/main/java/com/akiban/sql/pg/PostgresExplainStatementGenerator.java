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

package com.akiban.sql.pg;

import com.akiban.qp.exec.Plannable;
import com.akiban.server.error.UnableToExplainException;
import com.akiban.server.error.UnsupportedExplainException;
import com.akiban.sql.optimizer.OperatorCompiler;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.plan.PlanContext;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.ExplainStatementNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ParameterNode;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

/** SQL statement to explain another one. */
public class PostgresExplainStatementGenerator extends PostgresBaseStatementGenerator
{
    private OperatorCompiler compiler;

    public PostgresExplainStatementGenerator(PostgresServerSession server) {
        compiler = (OperatorCompiler)server.getAttribute("compiler");
    }

    @Override
    public PostgresStatement generate(PostgresServerSession server,
                                      StatementNode stmt, 
                                      List<ParameterNode> params,
                                      int[] paramTypes)  {
        if (stmt.getNodeType() != NodeTypes.EXPLAIN_STATEMENT_NODE)
            return null;
        StatementNode innerStmt = ((ExplainStatementNode)stmt).getStatement();
        if (compiler == null)
            throw new UnsupportedExplainException();
        if (!(innerStmt instanceof DMLStatementNode))
            throw new UnableToExplainException ();
        PlanContext plan = new PlanContext(compiler);
        Map<Object, Explainer> extraInfo = new HashMap<Object, Explainer>();
        plan.makeInfo(extraInfo);
        BasePlannable result = compiler.compile((DMLStatementNode)innerStmt, params, plan);
        return new PostgresExplainStatement(result.explainPlan(), compiler.usesPValues());
    }
}
