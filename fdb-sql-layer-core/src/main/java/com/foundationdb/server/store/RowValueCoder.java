/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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
package com.foundationdb.server.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.store.format.tuple.TupleStorageDescription;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.value.ValueTargets;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.HandleCache;
import com.persistit.encoding.ValueDisplayer;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;

/**
 * This was a two hour attempt to get the PersistitStorageDescription to perform
 * a Row -> Persistit.Value conversion without going through a RowData. 
 * 
 * I encountered at least one Persistit bug (noted below and in 
 * https://github.com/pbeaman/persistit/issues/2
 * 
 * This file (and its references) are unused. If we (the FDB team) determine
 * that we want to invest more time in the Persistit layer, fixing this should
 * be a priority. If not, this (and references) can be deleted. 
 * 
 * @author tjoneslo
 *
 */

public class RowValueCoder implements ValueDisplayer, ValueRenderer,
        HandleCache {

    @Override
    public Object get(Value value, Class<?> clazz, CoderContext context)
            throws ConversionException {
        SchemaCoderContext schema = (SchemaCoderContext)context;
        RowType rowType = schema.getRowType();
        Object[] objects = new Object[rowType.nFields()];
        for (int i = 0; i < rowType.nFields(); i++) {
            objects[i] = value.get(null, context);
        }
        ValuesHolderRow row = new ValuesHolderRow(rowType, objects);
        return row;
    }

    @Override
    public void put(Value value, Object source, CoderContext context)
            throws ConversionException {
        Row row = (Row)source;
        /**
         * TODO: This is a hack to get around a problem with 
         * persistit.Value#directPut() and an object which calls back into 
         * here which in turn uses Value#put() to insert several values. 
         * directPut() encodes an "Serialized Object" code, but doesn't update
         * an internal _serializedItemCount. When you call put(), it uses this
         * _serialisedItemCount to determine if there are duplicate value, 
         * and encode them (for conciseness). If _serializedItemCount is off,
         * the indexing back for the duplicates is wrong and the decoding fails
         * 
         * The value.toString() correctly updates the _serializedItemCount
         * See: https://github.com/pbeaman/persistit/issues/2
         */
        value.toString();
        // Encode the values as needed. 
        for (int i = 0; i < row.rowType().nFields(); i++) {
            Object obj = ValueSources.toObject(row.value(i));
            value.put(obj, context);
        }
    }

    @Override
    public int getHandle() {
        return handle;
    }

    @Override
    public void setHandle(int handle) {
        if (this.handle != 0 && this.handle != handle) {
            throw new IllegalStateException("Attempt to change handle from " + this.handle + " to " + handle);
        }
        this.handle = handle;
    }

    @Override
    public void render(Value value, Object target, Class<?> clazz, CoderContext context)
            throws ConversionException {
        
        LOG.error("rendering object??");
        ValuesHolderRow row = (ValuesHolderRow)target;
        
        for (int i = 0; i < row.rowType().nFields(); i++) {
            ValueTargets.copyFrom(ValueSources.fromObject(value.get()),
                    row.valueAt(i));
        }
    }

    @Override
    public void display(Value value, StringBuilder target, Class<?> clazz,
            CoderContext context) throws ConversionException {        
        final Object object = get(value, clazz, context);
        if (object instanceof Row) {
            final Row row = (Row) object;
            target.append(row.toString());
        } else {
            target.append(object);
        }
    }
    private volatile int handle;
    private static final Logger LOG = LoggerFactory.getLogger(RowValueCoder.class);
    
    public static class SchemaCoderContext implements CoderContext {
        private static final long serialVersionUID = 1L;
        private final Schema schema;
        private final Key key;
        private final Group group;
        
        public SchemaCoderContext (Schema schema, Group group, Key key) {
            this.schema = schema;
            this.key = key;
            this.group = group;
        }
        
        public Schema getSchema() {
            return schema;
        }
        
        public RowType getRowType() {
            Table table = TupleStorageDescription.tableFromOrdinals(group, key);
            return schema.tableRowType(table);
        }
    }
}
