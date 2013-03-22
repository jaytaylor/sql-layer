
package com.akiban.server.expression.std;

import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.OutOfRangeException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.expression.ExpressionComposer.NullTreating;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;

public class DistanceLatLonExpression extends AbstractCompositeExpression {

    @Scalar("distance_lat_lon")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer() {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException {
            int size = argumentTypes.size();
            if (size != 4) throw new WrongExpressionArityException(4, size);
            
            for (int n = 0; n < size; ++n)
                argumentTypes.setType(n, AkType.DECIMAL);
            return ExpressionTypes.DOUBLE;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList) {
            int size = arguments.size();
            if (size != 4) throw new WrongExpressionArityException(4, size);
            return new DistanceLatLonExpression(arguments);     
        }

        @Override
        public NullTreating getNullTreating() {
            return NullTreating.RETURN_NULL;        
        }
    };

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating() {
        return true;
    }
    
    private static class CompositeEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private final double UPPER_BOUND = 360;
        
        public CompositeEvaluation (List<? extends ExpressionEvaluation> childrenEvals)
        {
            super(childrenEvals);
        }
        
        @Override
        public ValueSource eval()
        {
            for (ExpressionEvaluation child: children()) {
                if (child.eval().isNull())
                    return NullValueSource.only();
            }
            
            double y1 = inRange(children().get(0).eval().getDecimal().doubleValue(), -90, 90);
            double x1 = inRange(children().get(1).eval().getDecimal().doubleValue(), -180, 180);
            double y2 = inRange(children().get(2).eval().getDecimal().doubleValue(), -90, 90);
            double x2 = inRange(children().get(3).eval().getDecimal().doubleValue(), -180, 180);
            
            x1 = getShift(x1);
            x2 = getShift(x2);
            
            double result = getShortestDistance(x1, x2, x1-x2, y1-y2);   
            valueHolder().putDouble(result);
            
            return valueHolder();
        }
        
        private double inRange(double x, double lowerBound, double upperBound) {
            if (x > upperBound || x < lowerBound) throw new OutOfRangeException(x + " should be in the range of " + lowerBound + " to " + 
                    upperBound + " inclusive.");
            return x;
        }
        
        private double getShift(double num) {
            return num += UPPER_BOUND / 2;
        }
        
        private double getShortestDistance(double arg1, double arg2, double X, double Y) {
            double wrapAroundX, noWrapAroundX;
            wrapAroundX = noWrapAroundX = Math.pow(X, 2.0);
            if (arg1 < arg2) wrapAroundX = Math.pow(UPPER_BOUND - arg2 + arg1, 2.0);
            else if (arg2 < arg1) wrapAroundX = Math.pow(UPPER_BOUND - arg1 + arg2, 2.0);
            return Math.sqrt(Math.min(wrapAroundX, noWrapAroundX) + Math.pow(Y, 2.0));
        }
    }
    
    @Override
    public String name()
    {
        return "DISTANCE_LAT_LON";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new CompositeEvaluation(childrenEvaluations());
    }
    
    private static AkType getType(List<? extends Expression> args) {
        if (args.size() != 4) throw new WrongExpressionArityException(4, args.size());
        return AkType.DOUBLE;
    }
    
    protected DistanceLatLonExpression(List<? extends Expression> children)
    {
        super(getType(children), children);
    }

}
