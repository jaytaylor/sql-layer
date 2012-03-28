/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
import java.util.List;

public class ArithOps 
{
    @Scalar("times")
    public static final ArithOpComposer MULTIPLY = new ArithOpComposer('*')
    {
       @Override
       public long evaluate (long one, long two)
       {
           return one * two;
       }
       
       @Override
       public double evaluate (double one, double two)
       {
           return one * two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two)
       {
           return one.multiply(two);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two)
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
    };
    
    @Scalar("minus")
    public static final ArithOpComposer MINUS = new ArithOpComposer('-')
    {
       @Override
       public long evaluate (long one, long two)
       {
           return one - two;
       }
       
       @Override
       public double evaluate (double one, double two)
       {
           return one - two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two)
       {
           return one.subtract(two);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two)
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
    };

    @Scalar("mod")
    public static final ArithOpComposer MOD = new ArithOpComposer ('%')
    {
        @Override
        public long evaluate(long one, long two)
        {
            if (two == 0)
                throw new DivisionByZeroException();
            return one % two;
        }

        @Override
        public double evaluate(double one, double two)
        {
            if (two == 0)
                throw new DivisionByZeroException();
            return one % two;
        }

        @Override
        public BigDecimal evaluate(BigDecimal one, BigDecimal two)
        {
            if (two.equals(BigDecimal.ZERO))
                throw new DivisionByZeroException();
            return one.remainder(two);
        }

        @Override
        public BigInteger evaluate(BigInteger one, BigInteger two)
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
       public long evaluate (long one, long two)
       {
           if (two == 0)
                throw new DivisionByZeroException();
           return one / two;
       }
       
       @Override
       public double evaluate (double one, double two) 
       {
           if (two == 0)
                throw new DivisionByZeroException();
           return one / two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two)
       {
           if (two.equals(BigDecimal.ZERO))
                throw new DivisionByZeroException();
           return one.divide(two);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two)
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
       public long evaluate (long one, long two)
       {
           if (two == 0)
                throw new DivisionByZeroException();
           return one / two;
       }
       
       @Override
       public double evaluate (double one, double two) 
       {
           if (two == 0)
                throw new DivisionByZeroException();
           return one / two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two)
       {
           if (two.equals(BigDecimal.ZERO))
                throw new DivisionByZeroException();
           return one.divide(two);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two)
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
    };
    
    @Scalar("plus")
    public static final ArithOpComposer ADD = new ArithOpComposer('+')
    {
        
       @Override
       public long evaluate (long one, long two)
       {  
           return one + two;
       }
       
       @Override
       public double evaluate (double one, double two)
       {
           return one + two;
       }
       
       @Override
       public BigDecimal evaluate (BigDecimal one, BigDecimal two)
       {
           return one.add(two);
       }
       
       @Override
       public BigInteger evaluate (BigInteger one, BigInteger two)
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
        public Expression compose (List<? extends Expression> args)
        {        
            switch(args.size())
            {
                case 2:   return new ArithExpression(args.get(0), this, args.get(1));
                case 1:   if (ArithExpression.isNumeric(args.get(0).valueType()))      // INT has the lowest precedence
                              return new ArithExpression(ZERO_INT, this, args.get(0)); // as far as ArithExp concerns
                default:  throw new WrongExpressionArityException(2, args.size());
            }
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
                  scale = Math.max(scale, 9); //sum of their precision isn't gonna be good enough
                  pre = Math.max(pre, pre - scale + 9);
                  break;
                case 'd':
                  scale = 0;
            }
            
            top = ArithExpression.getTopType(first.getType(), second.getType(), this);
            
            return ExpressionTypes.newType(top, pre, scale);
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
