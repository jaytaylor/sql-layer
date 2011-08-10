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

import java.math.BigDecimal;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.rowdata.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

public final class DecimalEncoder extends EncodingBase<BigDecimal> {
    DecimalEncoder() {
    }

    /**
     * Note: Only a "good guess". No way to determine how much room
     * key.append(BigDecimal) will take currently.
     */
    @Override
    public long getMaxKeyStorageSize(final Column column) {
        return column.getMaxStorageSize();
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        return fieldDef.getMaxStorageSize();
    }
}
