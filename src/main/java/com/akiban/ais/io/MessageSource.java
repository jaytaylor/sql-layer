package com.akiban.ais.io;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Source;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MessageSource extends Source
{
    // Source interface

    @Override
    public void close() throws Exception
    {
    }

    // MessageSource interface

    public MessageSource(ByteBuffer payload)
    {
        this.payload = payload;
    }

    @Override
    protected final void read(String typename, Receiver receiver) throws Exception
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
            s = new String(bytes);
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

    private final ByteBuffer payload;
}