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

public interface ConversionSource {
    public boolean isNull();
    BigDecimal getDecimal();
    BigInteger getUBigInt();
    ByteBuffer getVarBinary();
    double getDouble();
    double getUDouble();
    float getFloat();
    float getUFloat();
    long getDate();
    long getDateTime();
    long getInt();
    long getLong();
    long getTime();
    long getTimestamp();
    long getUInt();
    long getYear();
    String getString();
    String getText();

}
