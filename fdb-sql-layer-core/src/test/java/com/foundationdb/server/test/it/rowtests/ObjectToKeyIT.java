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

package com.foundationdb.server.test.it.rowtests;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.store.PersistitKeyAppender;
import com.foundationdb.server.test.it.ITBase;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class ObjectToKeyIT extends ITBase {
    private final String SCHEMA = "test";
    private final String TABLE = "t";

     private void testObjectToKey(Column field, Object... testValues) throws PersistitException {
        Key key = store().createKey();
        PersistitKeyAppender appender = PersistitKeyAppender.create(key, null);
        for(Object inObj : testValues) {
            key.clear();
            appender.append(inObj, field);

            Object outObj = key.decode();
            if(outObj != null) {
                assertEquals(inObj.toString(), outObj.toString());
            }
            else {
                assertEquals(inObj, outObj);
            }
        }
    }

    @Test
    public void decimalField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, false, false, new SimpleColumn("c2", "MCOMPAT_ decimal", 5L, 2L));
        final Column fieldDef = getTable(tid).getColumn("c2");
        testObjectToKey(fieldDef,
                        null, BigDecimal.valueOf(-12345, 2), 578L, "999.99");
    }

    @Test
    public void decimalUnsignedField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, false, true, new SimpleColumn("c2", "MCOMPAT_ decimal", 5L, 2L));
        final Column fieldDef = getTable(tid).getColumn("c2");
        testObjectToKey(fieldDef,
                        null, BigDecimal.valueOf(0), BigDecimal.valueOf(4242, 2), 489L, "978.83");
    }
}
