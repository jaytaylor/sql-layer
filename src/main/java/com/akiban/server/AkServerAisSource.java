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

package com.akiban.server;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Source;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.RowCollector;
import com.akiban.server.store.Store;

public class AkServerAisSource extends Source {

    private final Store store;

    private final Session session;
    
    public AkServerAisSource(final Store store, SessionService sessionService) {
        this.store = store;
        this.session = sessionService.createSession();
    }

    @Override
    public void close() {
        session.close();
    }

    @Override
    public int readVersion ()
    {
        return MetaModel.only().getModelVersion();
    }
    @Override
    protected final void read(String typename, Receiver receiver) {
        ModelObject modelObject = MetaModel.only().definition(typename);
        final RowDef rowDef = store.getRowDefCache().getRowDef(modelObject.tableName());
        if (rowDef == null) {
            throw new IllegalStateException(
                    "Missing table definition for AIS table "
                            + modelObject.name());
        }
        if (rowDef.getFieldCount() != modelObject.attributes().size()) {
            throw new IllegalStateException("RowDef for table "
                    + modelObject.name() + " does not match modelObject");
        }
        final RowData nullBounds = new RowData(new byte[64]);
        nullBounds.createRow(rowDef, new Object[0]);
        final RowCollector rowCollector = store.newRowCollector(session, 0, rowDef.getRowDefId(), 0,
                                                                null, null, null, null, null, null);
        final ByteBuffer buffer = ByteBuffer.allocate(65536);
        final RowData row = new RowData(buffer.array());
        rowCollector.open();
        while (rowCollector.hasMore()) {
            buffer.clear();
            if (rowCollector.collectNextRow(buffer)) {
                row.prepareRow(0);
                if (row.getRowDefId() != rowDef.getRowDefId()) {
                    throw new IllegalStateException(
                            "Stored row has unexpected RowDefId: expected="
                                    + rowDef.getRowDefId() + " actual="
                                    + row.getRowDefId());
                }
                readRow(rowDef, row, modelObject, receiver);
            }
        }
    }

    private void readRow(final RowDef rowDef, final RowData rowData,
            final ModelObject modelObject, final Receiver receiver) {
        final Map<String, Object> values = new HashMap<String, Object>();
        for (int index = 0; index < rowDef.getFieldCount(); index++) {
            final FieldDef fieldDef = rowDef.getFieldDef(index);
            final long location = rowDef.fieldLocation(rowData, index);
            final String attrName = modelObject.attributes().get(index).name();
            if (location == 0) {
                values.put(attrName, null);
            } else {
                switch (modelObject.attributes().get(index).type()) {
                case BOOLEAN: {
                    assert fieldDef.isFixedSize();
                    final int v = (int) rowData.getIntegerValue((int) location,
                            (int) (location >>> 32));
                    values.put(attrName, Boolean.valueOf(v != 0));
                    break;
                }
                case INTEGER: {
                    assert fieldDef.isFixedSize();
                    final int v = (int) rowData.getIntegerValue((int) location,
                            (int) (location >>> 32));
                    values.put(attrName, Integer.valueOf(v));
                    break;
                }
                case LONG: {
                    assert fieldDef.isFixedSize();
                    final long v = rowData.getIntegerValue((int) location,
                            (int) (location >>> 32));
                    values.put(attrName, Long.valueOf(v));
                    break;
                }
                case STRING: {
                    final String v = rowData.getStringValue((int) location,
                            (int) (location >>> 32), fieldDef);
                    values.put(attrName, v);
                    break;
                }
                default:
                    throw new Error("Missing case");
                }
            }
        }
        receiver.receive(values);
    }
}
