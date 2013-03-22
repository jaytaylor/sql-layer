/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.types3.pvalue;

import com.akiban.server.types3.TInstance;

public interface PBasicValueSource {
    TInstance tInstance();

    boolean isNull();

    boolean getBoolean();

    boolean getBoolean(boolean defaultValue);

    byte getInt8();

    short getInt16();

    char getUInt16();

    int getInt32();

    long getInt64();

    float getFloat();

    double getDouble();

    byte[] getBytes();

    String getString();
}
