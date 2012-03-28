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