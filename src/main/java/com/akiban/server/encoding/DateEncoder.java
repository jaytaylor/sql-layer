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

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import com.akiban.server.FieldDef;
import com.akiban.server.RowData;

public final class DateEncoder extends AbstractDateEncoder {
    DateEncoder() {
    }

    @Override
    public Date toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getLocation(fieldDef, rowData);

        final int v = (int) rowData.getIntegerValue((int) location, 3);
        final int year = v / (32 * 16);
        final int month = (v / 32) % 16;
        final int day = v % 32;

        final Calendar calendar = new GregorianCalendar();
        calendar.set(year - 1900, month -1, day);
        return calendar.getTime();
    }
}
