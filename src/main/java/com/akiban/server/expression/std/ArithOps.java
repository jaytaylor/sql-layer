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

package com.akiban.server.expression.std;

import com.akiban.server.error.DivisionByZeroException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.sql.StandardException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

public class ArithOps 
{
    @Scalar("times")
    public static final ArithOpComposer MULTIPLY = new ArithOpComposer('*')
    {
       @Override
       public long evaluate (long one, long two, ExpressionType exp)
       {
           return one * two;
       }
       
       @Override
       public double evaluate (double one, double two, ExpressionType exp)
       {
           return one * two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two, ExpressionType exp)
       {
           return one.multiply(two);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two, ExpressionType exp)
       {
           return one.multiply(two);
       }
       
        @Override
        protected void adjustVarchar(TypesList args, int index) throws StandardException
        {
            AkType type2 = args.get(index).getType();

            if (type2 == AkType.INTERVAL_MILLIS || type2 == AkType.INTERVAL_MONTH)
                args.setType(1 - index, AkType.DOUBLE);
        }

        @Override
        public boolean isInfix()
        {
            return true;
        }
        
        @Override
        public boolean isAssociative()
        {
            return true;
        }
        
    };
    
    @Scalar("minus")
    public static final ArithOpComposer MINUS = new ArithOpComposer('-')
    {
       @Override
       public long evaluate (long one, long two, ExpressionType exp)
       {
           return one - two;
       }
       
       @Override
       public double evaluate (double one, double two, ExpressionType exp)
       {
           return one - two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two, ExpressionType exp)
       {
           return one.subtract(two);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two, ExpressionType exp)
       {
           return one.subtract(two);
       }        
    
        @Override
        protected void adjustVarchar(TypesList args, int index) throws StandardException
        {
            AkType type2 = args.get(index).getType();
            switch (type2)
            {
                case DATE:
                case TIME:
                case DATETIME:
                case TIMESTAMP:
                case YEAR:
                    args.setType(1 - index, type2);
                    break;
                case INTERVAL_MILLIS:
                case INTERVAL_MONTH:
                    if (index == 0)
                        return; // INTERVAL can only be the rhs in substraction
                    args.setType(1 - index, args.get(1 - index).getPrecision() > 10 ? AkType.DATETIME : AkType.DATE);
            }
        }

        @Override
        public boolean isInfix()
        {
            return true;
        }
        
        @Override
        public boolean isAssociative()
        {
            return false;
        }
    };

    @Scalar("mod")
    public static final ArithOpComposer MOD = new ArithOpComposer ('%')
    {
        @Override
        public long evaluate(long one, long two, ExpressionType exp)
        {
            if (two == 0)
                throw new DivisionByZeroException();
            return one % two;
        }

        @Override
        public double evaluate(double one, double two, ExpressionType exp)
        {
            if (two == 0)
                throw new DivisionByZeroException();
            return one % two;
        }

        @Override
        public BigDecimal evaluate(BigDecimal one, BigDecimal two, ExpressionType exp)
        {
            if (two.equals(BigDecimal.ZERO))
                throw new DivisionByZeroException();
            return one.remainder(two);
        }

        @Override
        public BigInteger evaluate(BigInteger one, BigInteger two, ExpressionType exp)
        {
            if (two.equals(BigInteger.ZERO))
                throw new DivisionByZeroException();
            return one.remainder(two);
        }

        @Override
        protected void adjustVarchar(TypesList args, int index) throws StandardException
        {
            // does nothing, as MOD operation does not support any DATE/TIME
        }

        @Override
        public boolean isInfix()
        {
            return true;
        }
        
        @Override
        public boolean isAssociative()
        {
            return false;
        }
    };

    /**
     * "INTEGER DIVISION"
     * 
     *  The return type is:
     *          U_BIGINT if either of the two has approximate type
     *          whichever operands' type with higher precedence if both operands have exact type
     */
    @Scalar("div")
    public static final ArithOpComposer DIV = new ArithOpComposer('d')
    {
       @Override
       public long evaluate (long one, long two, ExpressionType exp)
       {
           if (two == 0)
                throw new DivisionByZeroException();
           return one / two;
       }
       
       @Override
       public double evaluate (double one, double two, ExpressionType exp) 
       {
           if (two == 0)
                throw new DivisionByZeroException();
           return Math.floor(one / two);
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two, ExpressionType exp)
       {
           if (two.equals(BigDecimal.ZERO))
                throw new DivisionByZeroException();
           return one.divide(two, 0, RoundingMode.FLOOR);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two, ExpressionType exp)
       {
           if (two.equals(BigInteger.ZERO))
                throw new DivisionByZeroException();
           return one.divide(two);
       } 
       
        @Override
        protected void adjustVarchar(TypesList args, int index) throws StandardException
        {
            if (index == 0)
                return; // INTERVAL can only be the rhs in a division
            AkType type = args.get(index).getType();

            if (type == AkType.INTERVAL_MILLIS || type == AkType.INTERVAL_MONTH)
                args.setType(1 - index, AkType.DOUBLE);
        }

        @Override
        public boolean isInfix()
        {
            return true;
        }
        
        @Override
        public boolean isAssociative()
        {
            return false;
        }
    };
    
    /**
     * "REGULAR DIVISION"
     * 
     * The return type is 
     *      DOUBLE if both operands are either of type DOUBLE or BIGINT/LONG/INT
     *      same as both operands' types, otherwise
     */
    @Scalar("divide")
    public static final ArithOpComposer DIVIDE = new ArithOpComposer('/')
    {
       @Override
       public long evaluate (long one, long two, ExpressionType exp)
       {
           if (two == 0)
                throw new DivisionByZeroException();
           return one / two;
       }
       
       @Override
       public double evaluate (double one, double two, ExpressionType exp) 
       {
           if (two == 0)
                throw new DivisionByZeroException();
           return one / two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two, ExpressionType exp)
       {
           if (two.equals(BigDecimal.ZERO))
                throw new DivisionByZeroException();
           return one.divide(two, exp.getScale(), RoundingMode.HALF_UP);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two, ExpressionType exp)
       {
           if (two.equals(BigInteger.ZERO))
                throw new DivisionByZeroException();
           return one.divide(two);
       } 
       
        @Override
        protected void adjustVarchar(TypesList args, int index) throws StandardException
        {
            if (index == 0)
                return; // INTERVAL can only be the rhs in a division
            AkType type = args.get(index).getType();

            if (type == AkType.INTERVAL_MILLIS || type == AkType.INTERVAL_MONTH)
                args.setType(1 - index, AkType.DOUBLE);
        }

        @Override
        public boolean isInfix()
        {
            return true;
        }
        
        @Override
        public boolean isAssociative()
        {
            return false;
        }
    };
    
    @Scalar("plus")
    public static final ArithOpComposer ADD = new ArithOpComposer('+')
    {
        
       @Override
       public long evaluate (long one, long two, ExpressionType exp)
       {  
           return one + two;
       }
       
       @Override
       public double evaluate (double one, double two, ExpressionType exp)
       {
           return one + two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two, ExpressionType exp)
       {
           return one.add(two);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two, ExpressionType exp)
       {
           return one.add(two);                                       
       }  
    
        @Override
        protected void adjustVarchar(TypesList args, int index) throws StandardException
        {
            AkType type2 = args.get(index).getType();
            index = 1 - index;

            if (type2 == AkType.INTERVAL_MILLIS || type2 == AkType.INTERVAL_MONTH)
                args.setType(index, args.get(index).getPrecision() > 10 ? AkType.DATETIME : AkType.DATE);
        }

        @Override
        public boolean isInfix()
        {
            return true;
        }
        
        @Override
        public boolean isAssociative()
        {
            return true;
        }
    };
            
    static abstract class ArithOpComposer implements ExpressionComposer, ArithOp
    {
        private static final Expression ZERO_INT = new LiteralExpression(AkType.INT, 0L);
        /**
         * 
         * @param args
         * @param index of the arg whose type is DATE/TIMES/NUMERIC (in short, anything but a VARCHAR or an UNSUPPORTED)
         * @throws StandardException 
         * 
         * adjust the VARCHAR arg to DATE, DATETIME or DOUBLE depending on the 
         * arg at [index]
         */
        protected abstract void adjustVarchar (TypesList args, int index) throws StandardException;
        
        protected Expression compose (Expression first, Expression second)
        {
            return new ArithExpression(first, this, second);
        }
        
        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }

        @Override
        public ExpressionType composeType(TypesList arguments) throws StandardException
        {
            AkType top;
            // unary ops (- or +) should only work with numeric types
            if (arguments.size() == 1 && (name == '-' || name == '+'))
            {
                if (ArithExpression.isNumeric(top = arguments.get(0).getType()))            
                    return arguments.get(0);                
                else if (top == AkType.VARCHAR)
                {                    
                    arguments.setType(0, AkType.DOUBLE); 
                    return arguments.get(0);
                }
            }
            
            if(arguments.size() != 2) throw new WrongExpressionArityException(2, arguments.size());
            ExpressionType first = arguments.get(0), second = arguments.get(1);
            
            // adjust some *ambiguous types* (VARCHAR and/or UNSUPPORTED (coming from params))
            if (first.getType() != second.getType())
            {
                int index; // index of the unambiguous argument
                if (    first.getType() == AkType.VARCHAR && 
                            ArithExpression.isDateTime(arguments.get(index = 1).getType()) ||
                        second.getType() == AkType.VARCHAR && 
                            !ArithExpression.isNumeric(arguments.get(index = 0).getType()))
                    // if one of the operands is VARCHAR and the other date/time/interval
                    // adjust the varchar to appropriate type
                    adjustVarchar(arguments, index);
                else if (   (first.getType() == AkType.UNSUPPORTED || first.getType() == AkType.VARCHAR)&&
                                ArithExpression.isNumeric(arguments.get(index = 1).getType()) ||
                            (second.getType() == AkType.UNSUPPORTED || second.getType() == AkType.VARCHAR)&& 
                                ArithExpression.isNumeric(arguments.get(index = 0).getType()))
                    // if one of the operands is param/varchar and the other numeric
                    // expect the parameter argument to have type DOUBLE
                    arguments.setType(1- index, AkType.DOUBLE);
                
                // update first, second
                first = arguments.get(0);
                second = arguments.get(1);
            }
            
            int scale = first.getScale() + second.getScale();
            int pre = first.getPrecision() + second.getPrecision();

            switch(name)
            {
                case '/':  // in case we have 2 LONG/INT/BIGINT values dividing each other, 
                  scale = Math.max(scale, ArithExpression.DEFAULT_SCALE); //sum of their precision isn't gonna be good enough
                  pre = Math.max(pre, pre - first.getScale() - second.getScale() + ArithExpression.DEFAULT_SCALE);
                  break;
                case 'd':
                  scale = 0;
            }
            
            top = ArithExpression.getTopType(first.getType(), second.getType(), this);
            return ExpressionTypes.newType(top, pre, scale);
        }

        @Override
        public Expression compose(List<? extends Expression> args, List<ExpressionType> typesList)
        {
            switch(args.size())
            {
            case 2:
                return new ArithExpression(args.get(0), this, args.get(1), typesList.get(0), typesList.get(1), typesList.get(2));
            case 1:
                if (ArithExpression.isNumeric(args.get(0).valueType()))      // INT has the lowest precedence
                    return new ArithExpression(ZERO_INT, this, args.get(0), ExpressionTypes.INT, typesList.get(0), typesList.get(1)); // as far as ArithExp concerns
            default:  
                throw new WrongExpressionArityException(2, args.size());
            }
        }
        
        @Override
        public String toString ()
        {
            return (name + "");
        }

        @Override
        public char opName () { return name;}

        protected ArithOpComposer (char name)
        {
            this.name = name;
        }
        
        private final char name;
    }
    
    private ArithOps() {}    
}
