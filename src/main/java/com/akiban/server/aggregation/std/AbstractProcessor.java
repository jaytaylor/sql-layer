
package com.akiban.server.aggregation.std;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.math.BigDecimal;
import java.math.BigInteger;

interface AbstractProcessor
{
    long process (long oldState, long input);
    double process (double oldState, double input);
    float process (float oldState, float input);
    BigDecimal process (BigDecimal oldState, BigDecimal input);
    BigInteger process (BigInteger oldState, BigInteger input);
    boolean process (boolean oldState, boolean input);
    String process (String oldState, String input);
    
    void checkType (AkType type);
    ValueSource emptyValue();
}
