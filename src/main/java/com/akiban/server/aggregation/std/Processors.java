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

package com.akiban.server.aggregation.std;

import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.OverflowException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigDecimal;
import java.math.BigInteger;

class Processors
{
    public final static AbstractProcessor bitAndProcessor = new BitProcessor ()
    {
        @Override
        public BigInteger process(BigInteger oldState, BigInteger input)
        {
            return oldState.and(input).and(n64);
        }

        @Override
        public String toString ()
        {
            return "BIT_AND";
        }

        @Override
        public ValueSource emptyValue()
        {
            return EMPTY_FOR_AND;
        }
    };

    public final static AbstractProcessor bitOrProcessor = new BitProcessor ()
    {
        @Override
        public BigInteger process(BigInteger oldState, BigInteger input)
        {
            return oldState.or(input).and(n64);
        }

        @Override
        public String toString ()
        {
            return "BIT_OR";
        }

        @Override
        public ValueSource emptyValue()
        {
            return EMPTY_FOR_OR;
        }
    };

    public final static AbstractProcessor bitXOrProcessor = new BitProcessor ()
    {
        @Override
        public BigInteger process(BigInteger oldState, BigInteger input)
        {
            return oldState.xor(input).and(n64);
        }

        @Override
        public String toString ()
        {
            return "BIT_XOR";
        }

        @Override
        public ValueSource emptyValue()
        {
            return EMPTY_FOR_OR;
        }
    };

    public final static AbstractProcessor maxProcessor = new MinMaxProcessor()
    {
        @Override
        public String toString ()
        {
            return "MAX";
        }

        @Override
        public boolean condition (double a) { return a > 0; }

    };

    public final static AbstractProcessor minProcessor = new MinMaxProcessor()
    {
        @Override
        public String toString ()
        {
            return "MIN";
        }

        @Override
        public boolean condition(double a) { return a < 0; }

    };

    public final static AbstractProcessor sumProcessor = new AbstractProcessor ()
    {   
        @Override
        public String toString ()
        {
            return "SUM";
        }
        @Override
        public void checkType (AkType type)
        {
            switch (type)
            {
                case DOUBLE:
                case U_DOUBLE:
                case U_INT:
                case FLOAT:
                case U_FLOAT:
                case INT:
                case LONG:
                case DECIMAL:
                case U_BIGINT: return;
                default:  throw new InvalidArgumentTypeException("Sum of " +type + " is not supported");
            }
        }

        @Override
        public long process(long oldState, long input)
        {
            long sum = oldState + input;
            if (oldState > 0 && input > 0 && sum <= 0)
                throw new OverflowException();
            else if (oldState < 0 && input < 0 && sum >= 0)
                throw new OverflowException();
            else
                return oldState + input;
        }

        @Override
        public double process(double oldState, double input)
        {
            double sum = oldState + input;  
            if (Double.isInfinite(sum) && !Double.isInfinite(oldState) && !Double.isInfinite(input))
                throw new OverflowException();
            else 
                return sum;
        }

        @Override
        public float process (float oldState, float input)
        {
            float sum = oldState  + input;
            if (Float.isInfinite(sum) && !Float.isInfinite(oldState) && !Float.isInfinite(input))
                throw new OverflowException();
            else
                    return sum;
        }

        @Override
        public BigDecimal process(BigDecimal oldState, BigDecimal input)
        {
            return oldState.add(input);
        }

        @Override
        public BigInteger process(BigInteger oldState, BigInteger input)
        {
            return oldState.add(input);
        }

        @Override
        public boolean process(boolean oldState, boolean input)
        {
            throw new InvalidArgumentTypeException("Sum of BOOL is not supported");
        }

        @Override
        public String process(String oldState, String input)
        {
             throw new InvalidArgumentTypeException("Sum of VARCHAR is not supported");
        }

        @Override
        public ValueSource emptyValue()
        {
            return NullValueSource.only();
        }
    };

    // nested class
    private static abstract class BitProcessor implements AbstractProcessor
    {
        protected static final BigInteger n64 = new BigInteger("FFFFFFFFFFFFFFFF", 16);
        protected static final ValueSource EMPTY_FOR_AND = new ValueHolder(AkType.U_BIGINT, n64);
        protected static final ValueSource EMPTY_FOR_OR = new ValueHolder(AkType.U_BIGINT, BigInteger.ZERO);

        @Override
        public long process(long oldState, long input)
        {
            throw new InvalidArgumentTypeException( toString() + " of LONG  is not supported. Cast to BigInt");
        }

        @Override
        public double process(double oldState, double input)
        {
            throw new InvalidArgumentTypeException( toString() + " of DOUBLE  is not supported. Cast to BigInt");
        }

        @Override
        public float process(float oldState, float input)
        {
            throw new InvalidArgumentTypeException( toString() + " of FLOAT  is not supported. Cast to BigInt");
        }

        @Override
        public BigDecimal process(BigDecimal oldState, BigDecimal input)
        {
            throw new InvalidArgumentTypeException( toString() + " of DECIMAL  is not supported. Cast to BigInt");
        }

        @Override
        public boolean process(boolean oldState, boolean input)
        {
            throw new InvalidArgumentTypeException( toString() + " of BOOLEAN  is not supported. Cast to BigInt");
        }

        @Override
        public String process(String oldState, String input)
        {
            throw new InvalidArgumentTypeException( toString() + " of VARCHAR  is not supported. Cast to BigInt");
        }

        @Override
        public void checkType(AkType type)
        {
            switch (type)
            {
                case DOUBLE:
                case U_DOUBLE:
                case U_INT:
                case FLOAT:
                case U_FLOAT:
                case INT:
                case LONG:
                case DECIMAL:
                case U_BIGINT: return;
                default:  throw new InvalidArgumentTypeException(toString() + " of " +type + " is not supported");
            }
        }
    }
    private static abstract class MinMaxProcessor implements AbstractProcessor
    {
        abstract boolean condition (double a);

        @Override
        public void checkType(AkType type)
        {
            switch (type)
            {
                case DOUBLE:
                case FLOAT:
                case INT:
                case U_FLOAT:
                case U_INT:
                case LONG:
                case DECIMAL:
                case U_BIGINT:
                case VARCHAR:
                case TEXT:
                case TIMESTAMP:
                case DATE:
                case BOOL:
                case DATETIME:
                case TIME:      return;
                default:        throw new UnsupportedOperationException(type + " is not supported yet.");
            }            
        }

        @Override
        public long process(long oldState, long input)
        {
            return (condition(oldState - input) ? oldState : input);
        }

        @Override
        public double process(double oldState, double input)
        {
            return (condition(oldState - input) ? oldState : input);
        }

        @Override
        public float process (float oldState, float input)
        {
            return (condition(oldState - input) ? oldState : input);
        }

        @Override
        public BigDecimal process(BigDecimal oldState, BigDecimal input)
        {
            return (condition(oldState.compareTo(input))? oldState : input);
        }

        @Override
        public BigInteger process(BigInteger oldState, BigInteger input)
        {
            return (condition(oldState.compareTo(input)) ? oldState : input);
        }

        @Override
        public boolean process(boolean oldState, boolean input)
        {
            return condition(1);
        }

        @Override
        public String process(String oldState, String input)
        {
            return (condition(oldState.compareTo(input)) ? oldState : input);
        }

        @Override
        public ValueSource emptyValue ()
        {
            return NullValueSource.only();
        }
    }
}
