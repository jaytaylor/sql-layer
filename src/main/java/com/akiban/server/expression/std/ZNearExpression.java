
package com.akiban.server.expression.std;

import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;

public class ZNearExpression extends AbstractCompositeExpression {

    @Scalar("znear")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer() {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException {
            int size = argumentTypes.size();
            if (size != 4) throw new WrongExpressionArityException(4, size);
            
            for (int i = 0; i < size; ++i) {
                argumentTypes.setType(i, AkType.DECIMAL);
            }
            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList) {
            int size = arguments.size();
            if (size != 4) throw new WrongExpressionArityException(4, size);
            return new ZNearExpression(arguments);     
        }

        @Override
        public ExpressionComposer.NullTreating getNullTreating() {
            return ExpressionComposer.NullTreating.RETURN_NULL;        
        }
    };
    
    private static class CompositeEvaluation extends AbstractCompositeExpressionEvaluation {        
        public CompositeEvaluation (List<? extends ExpressionEvaluation> childrenEvals) {
            super(childrenEvals);
        }
        
        @Override
        public ValueSource eval() {
            throw new UnsupportedSQLException("This query is not supported by Akiban, its definition " + 
                                              "is used solely for optimization purposes.");
        }
    }
    
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating() {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new CompositeEvaluation(childrenEvaluations());
    }

    @Override
    public String name() {
        return "ZNEAR";
    }
    
    private static List<? extends Expression> checkArity(List<? extends Expression> children) {
        int size = children.size();
        if (size != 4) throw new WrongExpressionArityException(4, size);
        return children;
    }
    
    protected ZNearExpression(List<? extends Expression> children) {
        super(AkType.LONG, checkArity(children));
    }

}
