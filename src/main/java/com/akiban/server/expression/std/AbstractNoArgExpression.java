
package com.akiban.server.expression.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Type;
import com.akiban.server.explain.std.ExpressionExplainer;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import java.util.List;
import java.util.Map;

public abstract class AbstractNoArgExpression implements Expression {

    // Expression interface
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new ExpressionExplainer(Type.FUNCTION, name(), context);
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean needsBindings() {
        return false;
    }

    @Override
    public boolean needsRow() {
        return false;
    }

    @Override
    public AkType valueType() {
        return type;
    }

    // for use by subclasses

    protected AbstractNoArgExpression(AkType type) {
        this.type = type;
    }

    // object interface

    @Override
    public String toString() {
        return name() + "()";
    }

    // object state

    private final AkType type;
}
