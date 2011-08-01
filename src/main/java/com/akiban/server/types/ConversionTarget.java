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

package com.akiban.server.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public interface ConversionTarget {
    void putNull();
    void putDate(long value);
    void putDateTime(long value);
    void putDecimal(BigDecimal value);
    void putDouble(double value);
    void putFloat(float value);
    void putInt(long value);
    void putLong(long value);
    void putString(String value);
    void putText(String value);
    void putTime(long value);
    void putTimestamp(long value);
    void putUBigInt(BigInteger value);
    void putUDouble(double value);
    void putUFloat(float value);
    void putUInt(long value);
    void putVarBinary(ByteBuffer value);
    void putYear(long value);

}
