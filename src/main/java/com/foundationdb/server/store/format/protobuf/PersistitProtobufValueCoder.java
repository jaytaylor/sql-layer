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

package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.rowdata.RowDefCache;
import com.foundationdb.server.store.PersistitStore;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DynamicMessage;

import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.HandleCache;
import com.persistit.encoding.ValueDisplayer;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;

import java.io.IOException;

/** Encode a <code>DynamicMessage</code> in Persistit in such a way
 * that the maintenance console has a chance of displaying it.
 */
// TODO: This costs a few bytes for every row. Is it worth it?
public class PersistitProtobufValueCoder implements ValueDisplayer, ValueRenderer, HandleCache {
    private final PersistitStore store;
    private volatile int handle;

    public PersistitProtobufValueCoder(PersistitStore store) {
        this.store = store;
    }

    @Override
    public void put(Value value, Object object, CoderContext context) throws ConversionException {
        PersistitProtobufRow holder = (PersistitProtobufRow)object;
        ProtobufRowDataConverter converter = holder.getConverter();
        value.put(converter.getTableId());
        int pos = value.getEncodedSize();

        DynamicMessage msg = holder.getMessage();
        int size = msg.getSerializedSize();
        value.ensureFit(size);
        CodedOutputStream cstr = CodedOutputStream.newInstance(value.getEncodedBytes(), pos, size);
        try {
            msg.writeTo(cstr);
        }
        catch (IOException ex) {
            throw new ConversionException(ex);
        }
        value.setEncodedSize(pos + size);
    }

    @Override
    // NOTE: Should only be used during debugging via Persistit tools.
    public Object get(Value value, Class<?> clazz, CoderContext context) throws ConversionException {
        int pos = value.getCursor();
        int tableId = value.getInt();
        Table root = RowDefCache.latestForDebugging().ais().getTable(tableId);
        if (root != null) {
            StorageDescription storage = root.getGroup().getStorageDescription();
            if (storage instanceof PersistitProtobufStorageDescription) {
                ProtobufRowDataConverter converter = ((PersistitProtobufStorageDescription)storage).ensureConverter(store);
                PersistitProtobufRow holder = new PersistitProtobufRow(converter, null);
                value.setCursor(pos);
                try {
                    render(value, holder, clazz, context);
                    return holder;
                }
                catch (Exception ex) {
                    // Since for debugging, fall back to raw bytes.
                }
            }
        }
        // Did not manage to decode via cached info. Just give raw bytes.
        pos = value.getCursor();
        int size = value.getEncodedSize();
        byte[] raw = new byte[size - pos];
        System.arraycopy(value.getEncodedBytes(), pos, raw, 0, raw.length);
        return raw;
    }

    @Override
    public void render(Value value, Object object, Class<?> clazz, CoderContext context)
            throws ConversionException {
        PersistitProtobufRow holder = (PersistitProtobufRow)object;
        ProtobufRowDataConverter converter = holder.getConverter();
        int tableId = value.getInt();
        assert (tableId == converter.getTableId());
        int pos = value.getCursor();
        int size = value.getEncodedSize();
        CodedInputStream cstr = CodedInputStream.newInstance(value.getEncodedBytes(), pos, size - pos);
        try {
            holder.setMessage(DynamicMessage.parseFrom(converter.getMessageType(), cstr));
        }
        catch (IOException ex) {
            throw new ConversionException(ex);
        }
    }

    @Override
    public void display(Value value, StringBuilder target, Class<?> clazz, CoderContext context)
            throws ConversionException {
        Object object = get(value, clazz, context);
        if (object instanceof PersistitProtobufRow) {
            PersistitProtobufRow holder = (PersistitProtobufRow)object;
            ProtobufRowDataConverter converter = holder.getConverter();
            try {
                converter.format(holder.getMessage(), target);
            }
            catch (IOException ex) {
                throw new ConversionException(ex);
            }
        }
        else if (object instanceof byte[]) {
            byte[] bytes = (byte[])object;
            for (int i = 0; i < bytes.length; i++) {
                byte b = bytes[i];
                target.append(Character.forDigit((b >> 4) & 0xF, 16));
                target.append(Character.forDigit(b & 0xF, 16));
            }
            return;
        }
        else {
            target.append(object);
        }
    }

    @Override
    public int getHandle() {
        return this.handle;
    }

    @Override
    public synchronized void setHandle(int handle) {
        this.handle = handle;
    }
}
