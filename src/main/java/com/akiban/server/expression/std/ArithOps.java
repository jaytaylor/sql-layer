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
import com.akiban.server.expression.Expression;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import java.math.BigDecimal;
import java.math.BigInteger;

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
           return new BigInteger(1, one.toByteArray()).multiply(two);// force one to be unsigned
                                                                // so that the result is unsigned
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
           return new BigInteger(1, one.toByteArray()).subtract(two);
       }        
    };
    
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
           return new BigInteger(1, one.toByteArray()).divide(two);
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
           return new BigInteger(1, one.toByteArray()).add(two);                                          
       }  
    };
            
    static abstract class ArithOpComposer extends BinaryComposer implements ArithOp
    {
        private AkType resType = AkType.LONG;
        
        @Override
        public void setResultType (AkType type)
        {
            resType = type;
        }
        
        @Override
        public AkType getResultType ()
        {
            return resType;
        }
        
        @Override
        protected Expression compose (Expression first, Expression second)
        {
            return new ArithExpression(first, this, second);
        }
        
        @Override
        public String toString ()
        {
            return (name + " " + resType);
        }
        
        protected ArithOpComposer (char name)
        {
            this.name = name;
        }
        
        private final char name;
    }
    
    private ArithOps() {}    
}
