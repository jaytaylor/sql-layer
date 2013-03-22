package com.akiban.server.types3.common;

import com.akiban.server.types3.DeepCopiable;
import java.math.BigDecimal;

public interface BigDecimalWrapper extends Comparable<BigDecimalWrapper>, DeepCopiable<BigDecimalWrapper>
{
    
    BigDecimalWrapper set(BigDecimalWrapper value);
    BigDecimalWrapper add(BigDecimalWrapper addend);
    BigDecimalWrapper subtract(BigDecimalWrapper subtrahend);
    BigDecimalWrapper multiply(BigDecimalWrapper multiplicand);
    BigDecimalWrapper divide(BigDecimalWrapper divisor);
    BigDecimalWrapper floor();
    BigDecimalWrapper ceil();
    BigDecimalWrapper truncate(int scale);
    BigDecimalWrapper round(int scale);
    BigDecimalWrapper divideToIntegralValue(BigDecimalWrapper divisor);
    BigDecimalWrapper divide(BigDecimalWrapper divisor, int scale);
    BigDecimalWrapper parseString(String num);
    BigDecimalWrapper round (int precision, int scale);
    BigDecimalWrapper negate();
    BigDecimalWrapper abs();
    BigDecimalWrapper mod(BigDecimalWrapper num);
    BigDecimalWrapper deepCopy();
    
    int compareTo (BigDecimalWrapper o);
    int getScale();
    int getPrecision();
    int getSign();
    boolean isZero();
    void reset();

    BigDecimal asBigDecimal();
}
