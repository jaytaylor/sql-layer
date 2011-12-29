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

import com.akiban.server.error.AkibanInternalException;

import static com.akiban.server.types.AkType.UnderlyingType.*;

public enum AkType {
    DATE(LONG_AKTYPE),
    DATETIME(LONG_AKTYPE),
    DECIMAL(OBJECT_AKTYPE),
    DOUBLE(DOUBLE_AKTYPE),
    FLOAT(FLOAT_AKTYPE),
    INT(LONG_AKTYPE),
    LONG(LONG_AKTYPE),
    VARCHAR(OBJECT_AKTYPE),
    TEXT(OBJECT_AKTYPE),
    TIME(LONG_AKTYPE),
    TIMESTAMP(LONG_AKTYPE),
    U_BIGINT(OBJECT_AKTYPE),
    U_DOUBLE(DOUBLE_AKTYPE),
    U_FLOAT(FLOAT_AKTYPE),
    U_INT(LONG_AKTYPE),
    VARBINARY(OBJECT_AKTYPE),
    YEAR(LONG_AKTYPE),
    BOOL(BOOLEAN_AKTYPE),
    INTERVAL_MILLIS(LONG_AKTYPE),
    INTERVAL_MONTH(LONG_AKTYPE),
    RESULT_SET(OBJECT_AKTYPE),
    NULL(null),
    UNSUPPORTED(null),
    ;

    public UnderlyingType underlyingTypeOrNull() {
        return underlyingType;
    }
    
    public UnderlyingType underlyingType() {
        if (underlyingType == null) {
            throw new AkibanInternalException("no underlying type for " + name());
        }
        return underlyingType;
    }

    private AkType(UnderlyingType underlyingType) {
        this.underlyingType = underlyingType;
    }

    private final UnderlyingType underlyingType;

    public enum UnderlyingType {
        BOOLEAN_AKTYPE,
        LONG_AKTYPE,
        FLOAT_AKTYPE,
        DOUBLE_AKTYPE,
        OBJECT_AKTYPE
    }
}