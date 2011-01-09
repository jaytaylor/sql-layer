package com.akiban.cserver;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Source;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.store.RowCollector;
import com.akiban.cserver.store.Store;

public class CServerAisSource extends Source {

    private final Store store;

    private final Session session = new SessionImpl();
    
    public CServerAisSource(final Store store) throws Exception {
        this.store = store;
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    protected final void read(String typename, Receiver receiver)
            throws Exception {
        ModelObject modelObject = MetaModel.only().definition(typename);
        final RowDef rowDef = store.getRowDefCache().getRowDef(
                modelObject.tableName());
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
        final RowCollector rowCollector = store.newRowCollector(session, rowDef
                .getRowDefId(), 0, 0, nullBounds, nullBounds, new byte[] {
                (byte) 0xFF, (byte) 0xFF });
        final ByteBuffer buffer = ByteBuffer.allocate(65536);
        final RowData row = new RowData(buffer.array());
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
            final ModelObject modelObject, final Receiver receiver)
            throws Exception {
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
