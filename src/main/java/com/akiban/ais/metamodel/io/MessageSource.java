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

package com.akiban.ais.metamodel.io;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.metamodel.Source;

public class MessageSource extends Source
{
    // Source interface

    @Override
    public void close()
    {
    }

    // MessageSource interface

    public MessageSource(ByteBuffer payload)
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

    private final ByteBuffer payload;
}