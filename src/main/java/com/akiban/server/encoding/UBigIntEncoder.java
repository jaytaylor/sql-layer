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

package com.akiban.server.encoding;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.persistit.Key;

import java.math.BigInteger;

public class UBigIntEncoder extends EncodingBase<BigInteger> {
    UBigIntEncoder() {
    }

    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        return fieldDef.getMaxStorageSize();
    }

    /**
     * See {@link Key#appendBigInteger(BigInteger)}
     */
    @Override
    public long getMaxKeyStorageSize(Column column) {
        return (65/24) + 1;
    }
}
