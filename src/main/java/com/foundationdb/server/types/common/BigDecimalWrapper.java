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

import com.foundationdb.server.types.DeepCopiable;
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
