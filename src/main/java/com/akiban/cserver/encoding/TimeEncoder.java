package com.akiban.cserver.encoding;

import com.akiban.ais.model.Type;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;

import java.util.Date;

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
