package com.akiban.ais.io;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Target;

import java.nio.ByteBuffer;
import java.util.Map;

public class MessageTarget extends Target
{
    // Target interface

    @Override
    public void deleteAll() throws Exception
    {
    }

    @Override
    public void writeCount(int count) throws Exception
    {
        writeInt(count);
    }

    @Override
    public void close() throws Exception
    {
    }

    // PersistitTarget interface

    public MessageTarget(ByteBuffer payload)
    {
        this.payload = payload;
    }

    // For use by this class

    @Override
    protected final void write(String typename, Map<String, Object> map) throws Exception
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
            payload.putInt(s.length());
            payload.put(s.getBytes());
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

    private ByteBuffer payload;
}