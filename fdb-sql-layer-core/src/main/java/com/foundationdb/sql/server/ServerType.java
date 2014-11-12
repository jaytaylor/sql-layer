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

package com.foundationdb.sql.server;

import com.foundationdb.server.types.TInstance;

/** A type according to the server's regime.
 */
public abstract class ServerType
{
    public enum BinaryEncoding {
        NONE, INT_8, INT_16, INT_32, INT_64, FLOAT_32, FLOAT_64, STRING_BYTES,
        BINARY_OCTAL_TEXT, BOOLEAN_C, 
        TIMESTAMP_FLOAT64_SECS_2000_NOTZ, TIMESTAMP_INT64_MICROS_2000_NOTZ,
        DAYS_2000, TIME_FLOAT64_SECS_NOTZ, TIME_INT64_MICROS_NOTZ,
        DECIMAL_PG_NUMERIC_VAR, UUID
    }

    private final TInstance type;

    // TODO ensure this is never called with null
    protected ServerType(TInstance type) {
        this.type = type;
    }
    
    public TInstance getType() {
        return type;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.NONE;
    }

    @Override
    public String toString() {
        if (type == null)
            return "null";
        else
            return type.toStringConcise(false);
    }

}
