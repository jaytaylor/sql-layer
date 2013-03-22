
package com.akiban.server.expression.std;

import com.akiban.server.expression.ExpressionType;
import java.math.BigDecimal;
import java.math.BigInteger;


public interface ArithOp 
{  
    // long 
    long evaluate (long one, long two, ExpressionType exp);
    
    // double
    double evaluate (double one, double two, ExpressionType exp);
    
    // BigDecimal
    BigDecimal evaluate (BigDecimal one, BigDecimal two, ExpressionType exp);
    
    // BigInteger
    BigInteger evaluate (BigInteger one, BigInteger two, ExpressionType exp);

    abstract char opName ();
    
    boolean isInfix();
    
    boolean isAssociative();
}
