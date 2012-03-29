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

package com.akiban.ais.metamodel.io;

import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.metamodel.Source;
import com.akiban.util.GrowableByteBuffer;

public class MessageSource extends Source
{
    // Source interface

    @Override
    public void close()
    {
    }

    // MessageSource interface

    public MessageSource(GrowableByteBuffer payload)
    {
        this.payload = payload;
    }

    @Override
    public int readVersion ()
    {
        return readInt();
    }
    
    @Override
    protected final void read(String typename, Receiver receiver)
    {
        ModelObject modelObject = MetaModel.only().definition(typename);
        int count = readInt();
        for (int i = 0; i < count; i++) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (ModelObject.Attribute attribute : modelObject.attributes()) {
                Object value = null;
                switch (attribute.type()) {
                    case INTEGER:
                        value = readIntOrNull();
                        break;
                    case LONG:
                        value = readLongOrNull();
                        break;
                    case BOOLEAN:
                        value = readBooleanOrNull();
                        break;
                    case STRING:
                        value = readStringOrNull();
                        break;
                    default:
                        assert false;
                }
                map.put(attribute.name(), value);
            }
            receiver.receive(map);
        }
    }

    private String readStringOrNull()
    {
        String s = null;
        int length = payload.getInt();
        if (length >= 0) {
            byte[] bytes = new byte[length];
            payload.get(bytes);
            try {
              s = new String(bytes, "UTF-8");
            }
            catch (java.io.UnsupportedEncodingException ex) {
                throw new RuntimeException(ex);
            }
        }
        return s;
    }

    private Integer readIntOrNull()
    {
        Integer i = null;
        byte isNull = payload.get();
        if (isNull == 0) {
            i = payload.getInt();
        }
        return i;
    }

    private Integer readInt()
    {
        return payload.getInt();
    }

    private Long readLongOrNull()
    {
        Long l = null;
        byte isNull = payload.get();
        if (isNull == 0) {
            l = payload.getLong();
        }
        return l;
    }

    private Boolean readBooleanOrNull()
    {
        byte b = payload.get();
        return (b == -1) ? null : (b != 0);
    }

    // State

    private final GrowableByteBuffer payload;
}