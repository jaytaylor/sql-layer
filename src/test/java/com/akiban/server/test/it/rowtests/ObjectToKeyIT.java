
package com.akiban.server.test.it.rowtests;

import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.store.PersistitKeyAppender;
import com.akiban.server.test.it.ITBase;
import com.persistit.Key;
import com.persistit.exception.PersistitException;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class ObjectToKeyIT extends ITBase {
    private final String SCHEMA = "test";
    private final String TABLE = "t";

    @Override
    protected boolean testSupportsPValues() {
        return false;
    }

    private void testObjectToKey(FieldDef field, Object... testValues) throws PersistitException {
        Key key = persistitStore().getKey();
        PersistitKeyAppender appender = PersistitKeyAppender.create(key);
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
        final int tid = createTableFromTypes(SCHEMA, TABLE, false, false, new SimpleColumn("c2", "decimal", 5L, 2L));
        final FieldDef fieldDef = getUserTable(tid).getColumn("c2").getFieldDef();
        testObjectToKey(fieldDef,
                        null, BigDecimal.valueOf(-12345, 2), 578L, "999.99");
    }

    @Test
    public void decimalUnsignedField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, false, true, new SimpleColumn("c2", "decimal", 5L, 2L));
        final FieldDef fieldDef = getUserTable(tid).getColumn("c2").getFieldDef();
        testObjectToKey(fieldDef,
                        null, BigDecimal.valueOf(0), BigDecimal.valueOf(4242, 2), 489L, "978.83");
    }
}
