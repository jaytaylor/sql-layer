
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;
import org.joda.time.DateTime;

public class UnixTimestampExpression extends AbstractCompositeExpression
{
    @Scalar("unix_timestamp")
    public static final ExpressionComposer COMPOSER= new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {   
            switch(argumentTypes.size())
            {
                case 1:     argumentTypes.setType(0, AkType.TIMESTAMP); // fall thru;
                case 0:     break;
                default:    throw new WrongExpressionArityException(2, argumentTypes.size());
            }

            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new UnixTimestampExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
    };

    @Override
    public String name() {
        return "TIMESTAMP";
    }
            
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            switch(children().size())
            {
                case 1:
                    ValueSource ts = children().get(0).eval();
                    if (ts.isNull())
                        return NullValueSource.only();
                    
                    long secs = ts.getTimestamp();
                    valueHolder().putLong(secs <= 0L ? 0L : secs);
                    break;
                case 0: // if called w/o argument, returns the current timestamp (similar to current_timestamp
                    valueHolder().putLong(new DateTime(queryContext().getCurrentDate()).getMillis() / 1000L);
                    break;
                default:
                    throw new WrongExpressionArityException(1, children().size());
            }
            return valueHolder();
        }
    }
    
    UnixTimestampExpression(List<? extends Expression> args)
    {
        super(AkType.LONG, args);
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
