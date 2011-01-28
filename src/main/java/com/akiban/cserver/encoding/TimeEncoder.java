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

package com.akiban.cserver.encoding;

import java.util.Date;

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;

public final class TimeEncoder extends AbstractDateEncoder {
    TimeEncoder() {
    }

    @Override
    public Date toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getLocation(fieldDef, rowData);
        final int v = (int) rowData.getIntegerValue((int) location, 3);
        // http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
        final int day = v / 1000000;
        final int hour = (v / 10000) % 100;
        final int minute = (v / 100) % 100;
        final int second = v % 100;
        return new Date(0, 0, day, hour, minute, second);
    }
}
