
package com.akiban.sql.optimizer.plan;

import static com.akiban.server.service.text.FullTextQueryBuilder.BooleanType;

import java.util.ArrayList;
import java.util.List;

public class FullTextBoolean extends FullTextQuery
{
    private List<FullTextQuery> operands;
    private List<BooleanType> types;

    public FullTextBoolean(List<FullTextQuery> operands, List<BooleanType> types) {
        this.operands = operands;
        this.types = types;
    }

    public List<FullTextQuery> getOperands() {
        return operands;
    }
    public List<BooleanType> getTypes() {
        return types;
    }

    public boolean accept(ExpressionVisitor v) {
        for (FullTextQuery operand : operands) {
            if (!operand.accept(v)) {
                return false;
            }
        }
        return true;
    }

    public void accept(ExpressionRewriteVisitor v) {
        for (FullTextQuery operand : operands) {
            operand.accept(v);
        }
    }

    public FullTextBoolean duplicate(DuplicateMap map) {
        List<FullTextQuery> newOperands = new ArrayList<>(operands.size());
        for (FullTextQuery operand : operands) {
            newOperands.add((FullTextQuery)operand.duplicate(map));
        }
        return new FullTextBoolean(newOperands, new ArrayList<>(types));
    }
    
    
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("[");
        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                str.append(", ");
            }
            str.append(types.get(i));
            str.append("(");
            str.append(operands.get(i));
            str.append(")");
        }
        str.append("]");
        return str.toString();
    }

}
