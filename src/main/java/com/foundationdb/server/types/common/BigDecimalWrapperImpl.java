/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class BigDecimalWrapperImpl implements BigDecimalWrapper {

    public static final BigDecimalWrapperImpl ZERO = new BigDecimalWrapperImpl(BigDecimal.ZERO);

    private BigDecimal value;

    public BigDecimalWrapperImpl(BigDecimal value) {
        this.value = value;
    }

    public BigDecimalWrapperImpl(String num)
    {
        value = new BigDecimal(num);
    }

    public BigDecimalWrapperImpl(long val)
    {
        value = BigDecimal.valueOf(val);
    }

    public BigDecimalWrapperImpl()
    {
        value = BigDecimal.ZERO;
    }

    @Override
    public void reset() {
        value = BigDecimal.ZERO;
    }
            
    @Override
    public BigDecimalWrapper set(BigDecimalWrapper other) {
        value = other.asBigDecimal();
        return this;
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
    public BigDecimalWrapper divide(BigDecimalWrapper divisor, int scale)
    {
        value = value.divide(divisor.asBigDecimal(),
                scale,
                RoundingMode.HALF_UP);
        return this;
    }

    @Override
    public BigDecimalWrapper divideToIntegralValue(BigDecimalWrapper divisor)
    {
        value = value.divideToIntegralValue(divisor.asBigDecimal());
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

    @Override
    public String toString() {
        return value == null ? "UNSET" : value.toString();
    }

    @Override
    public BigDecimalWrapper deepCopy()
    {
        return new BigDecimalWrapperImpl(value);
    }
}

