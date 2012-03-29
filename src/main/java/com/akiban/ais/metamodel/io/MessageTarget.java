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

import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.metamodel.Target;
import com.akiban.util.GrowableByteBuffer;

public class MessageTarget extends Target
{
    // Target interface

    @Override
    public void deleteAll() 
    {
    }

    @Override
    public void writeCount(int count)
    {
        writeInt(count);
    }

    @Override
    public void close()
    {
    }

    // PersistitTarget interface

    public MessageTarget(GrowableByteBuffer payload)
    {
        this.payload = payload;
    }

    // For use by this class

    @Override 
    public void writeVersion(int modelVersion)
    {
        writeInt(modelVersion);
    }

    @Override
    protected final void write(String typename, Map<String, Object> map)
    {
        ModelObject modelObject = MetaModel.only().definition(typename);
        for (ModelObject.Attribute attribute : modelObject.attributes()) {
            switch (attribute.type()) {
                case INTEGER:
                    writeIntOrNull((Integer) map.get(attribute.name()));
                    break;
                case LONG:
                    writeLongOrNull((Long) map.get(attribute.name())    );
                    break;
                case STRING:
                    writeStringOrNull((String) map.get(attribute.name()));
                    break;
                case BOOLEAN:
                    writeBooleanOrNull((Boolean) map.get(attribute.name()));
                    break;
            }
        }
    }

    private void writeStringOrNull(String s)
    {
        if (s == null) {
            payload.putInt(NULL_STRING);
        } else {
            byte[] bytes;
            try {
                bytes = s.getBytes("UTF-8");
            }
            catch (java.io.UnsupportedEncodingException ex) {
                throw new RuntimeException(ex);
            }
            payload.putInt(bytes.length);
            payload.put(bytes);
        }
    }

    private void writeIntOrNull(Integer i)
    {
        if (i == null) {
            payload.put(IS_NULL);
        } else {
            payload.put(IS_NOT_NULL);
            payload.putInt(i);
        }
    }

    private void writeInt(Integer i)
    {
        payload.putInt(i);
    }

    private void writeLongOrNull(Long l)
    {
        if (l == null) {
            payload.put(IS_NULL);
        } else {
            payload.put(IS_NOT_NULL);
            payload.putLong(l);
        }
    }

    private void writeBooleanOrNull(Boolean b)
    {
        payload.put(b == null ? (byte) -1 : b ? (byte) 1 : (byte) 0);
    }

    // State

    private static int NULL_STRING = -1;
    private static byte IS_NULL = (byte) 1;
    private static byte IS_NOT_NULL = (byte) 0;

    private GrowableByteBuffer payload;
}