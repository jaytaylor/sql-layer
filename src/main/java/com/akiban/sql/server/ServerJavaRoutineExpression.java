
package com.akiban.sql.server;

import com.akiban.ais.model.Routine;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.AbstractCompositeExpression;

import java.util.List;

public abstract class ServerJavaRoutineExpression extends AbstractCompositeExpression {
    protected Routine routine;

    protected ServerJavaRoutineExpression(Routine routine,
                                          List<? extends Expression> children) {
        super(routine.getReturnValue().getType().akType(), children);
        this.routine = routine;
    }

    @Override
    public String name () {
        return routine.getName().toString();
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(name());
    }

    @Override
    public boolean isConstant() {
        return routine.isDeterministic() && super.isConstant();
    }

    @Override
    public boolean nullIsContaminating() {
        return !routine.isCalledOnNullInput();
    }

}
