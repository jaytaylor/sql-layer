
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.unparser.NodeToString;

import java.util.List;

/** A parsed (and type-bound, normalized, etc.) SQL query.
 */
public class AST extends BasePlanNode
{
    private DMLStatementNode statement;
    private List<ParameterNode> parameters;

    public AST(DMLStatementNode statement, List<ParameterNode> parameters) {
        this.statement = statement;
        this.parameters = parameters;
    }
    
    public DMLStatementNode getStatement() {
        return statement;
    }

    public List<ParameterNode> getParameters() {
        return parameters;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }
    
    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        try {
            str.append(new NodeToString().toString(statement));
        }
        catch (StandardException ex) {
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy AST.
    }

}
