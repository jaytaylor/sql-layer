package com.akiban.cserver.encoding;

import com.akiban.ais.model.Type;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

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
