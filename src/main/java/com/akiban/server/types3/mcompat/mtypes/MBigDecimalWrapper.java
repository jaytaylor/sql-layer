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

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.types3.common.BigDecimalWrapper;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class MBigDecimalWrapper implements BigDecimalWrapper {

    public static final MBigDecimalWrapper ZERO = new MBigDecimalWrapper(BigDecimal.ZERO);

    private BigDecimal value;

    public MBigDecimalWrapper(BigDecimal value) {
        this.value = value;
    }

    public MBigDecimalWrapper(String num)
    {
        value = new BigDecimal(num);
    }

    public MBigDecimalWrapper(long val)
    {
        value = BigDecimal.valueOf(val);
    }

    public MBigDecimalWrapper()
    {
        value = BigDecimal.ZERO;
    }

    @Override
    public void reset() {
        value = BigDecimal.ZERO;
    }
            
    @Override
    public BigDecimalWrapper add(BigDecimalWrapper other) {
        value = value.add(other.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper subtract(BigDecimalWrapper other) {
        value = value.subtract(other.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper multiply(BigDecimalWrapper other) {
        value = value.multiply(other.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper divide(BigDecimalWrapper other) {
        value = value.divide(other.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper ceil() {
        value = value.setScale(0, RoundingMode.CEILING);
        return this;
    }
    
    @Override
    public BigDecimalWrapper floor() {
        value = value.setScale(0, RoundingMode.FLOOR);
        return this;
    }
    
    @Override
    public BigDecimalWrapper truncate(int scale) {
        value = value.setScale(scale, RoundingMode.DOWN);
        return this;
    }
    
    @Override
    public BigDecimalWrapper round(int scale) {
        value = value.setScale(scale, RoundingMode.HALF_UP);
        return this;
    }
    
    @Override
    public int getSign() {
        return value.signum();
    }
    
    @Override
    public BigDecimalWrapper divide(BigDecimalWrapper augend, int scale)
    {
        value = value.divide(augend.asBigDecimal(),
                scale,
                RoundingMode.UP);
        return this;
    }

    @Override
    public BigDecimalWrapper divideToIntegeralValue (BigDecimalWrapper augend)
    {
        value = value.divideToIntegralValue(augend.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper abs()
    {
        value = value.abs();
        return this;
    }
    
    @Override
    public int getScale()
    {
        return value.scale();
    }

    @Override
    public int getPrecision()
    {
        return value.precision();
    }

    @Override
    public BigDecimalWrapper parseString(String num)
    {
        value = new BigDecimal (num);
        return this;
    }

    @Override
    public int compareTo(BigDecimalWrapper o)
    {
        return value.compareTo(o.asBigDecimal());
    }

    @Override
    public BigDecimalWrapper round(int precision, int scale)
    {
        value = value.round(new MathContext(precision, RoundingMode.HALF_UP));
        return this;
    }

    @Override
    public BigDecimalWrapper negate()
    {
        value = value.negate();
        return this;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return value;
    }

    @Override
    public boolean isZero()
    {
        return value.signum() == 0;
    }

    @Override
    public BigDecimalWrapper mod(BigDecimalWrapper num)
    {
        value = value.remainder(num.asBigDecimal());
        return this;
    }
}

