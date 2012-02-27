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

package com.akiban.server.test.it.rowtests;

import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.encoding.Encoding;
import com.akiban.server.store.PersistitKeyAppender;
import com.akiban.server.test.it.ITBase;
import com.persistit.Key;
import com.persistit.exception.PersistitException;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class ObjectToKeyIT extends OldTypeITBase {

    private void testObjectToKey(FieldDef field, Object... testValues) throws PersistitException {
        Key key = persistitStore().getKey(session());
        PersistitKeyAppender appender = new PersistitKeyAppender(key);
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
        final int tid = createTableFromTypes(new TypeAndParams("decimal", 5L, 2L));
        final FieldDef fieldDef = (FieldDef)getUserTable(tid).getColumn("c2").getFieldDef();
        testObjectToKey(fieldDef,
                        null, BigDecimal.valueOf(-12345, 2), 578L, "999.99");
    }

    @Test
    public void decimalUnsignedField() throws Exception {
        setCreateIndexes(true);
        final int tid = createTableFromTypes(new TypeAndParams("decimal", 5L, 2L));
        final FieldDef fieldDef = (FieldDef)getUserTable(tid).getColumn("c2").getFieldDef();
        testObjectToKey(fieldDef,
                        null, BigDecimal.valueOf(0), BigDecimal.valueOf(4242, 2), 489L, "978.83");
    }
}
