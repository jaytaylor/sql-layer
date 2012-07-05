/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
