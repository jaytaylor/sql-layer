package com.akiban.cserver;

import java.sql.SQLException;
import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Target;
import com.akiban.cserver.store.Store;

/**
 * Implements the Target interface. This class is intended only for unit tests.
 * In the normal system life-cyle the AIS data will be inserted through normal
 * SQL calls to the MySQL head which will in turn sets WriteRowRequest messages.
 * 
 * @author peter
 * 
 */
public class CServerAisTarget extends Target {

    private final Store store;

    // Target interface

    public void deleteAll() throws Exception {
        deleteTable(type);
        deleteTable(group);
        deleteTable(table);
        deleteTable(column);
        deleteTable(join);
        deleteTable(joinColumn);
        deleteTable(index);
        deleteTable(indexColumn);
    }

    private void deleteTable(final String name) throws Exception {
        final ModelObject modelObject = MetaModel.only().definition(name);
        final RowDef rowDef = store.getRowDefCache().getRowDef(
                modelObject.tableName());
        if (rowDef == null) {
            throw new IllegalStateException(
                    "Missing table definition for AIS table " + name);
        }
        store.dropTable(rowDef.getGroupRowDefId());
    }

    @Override
    public void writeCount(int count) throws Exception {
    }

    public void close() throws SQLException {
    }

    public CServerAisTarget(final Store store) {
        this.store = store;
    }

    // For use by this class

    @Override
    protected final void write(String typename, Map<String, Object> map)
            throws Exception {
        ModelObject modelObject = MetaModel.only().definition(typename);
        final RowDef rowDef = store.getRowDefCache().getRowDef(
                modelObject.tableName());
        if (rowDef == null) {
            throw new IllegalStateException(
                    "Missing table definition for AIS table "
                            + modelObject.name());
        }
        final Object[] values = new Object[rowDef.getFieldCount()];

        for (int index = 0; index < modelObject.attributes().size(); index++) {
            final ModelObject.Attribute attribute = modelObject.attributes()
                    .get(index);

            switch (attribute.type()) {
            case INTEGER:
                values[index] = (Integer) map.get(attribute.name());
                break;
            case LONG:
                values[index] = (Long) map.get(attribute.name());
                break;
            case STRING:
                values[index] = (String) map.get(attribute.name());
                break;
            case BOOLEAN:
                values[index] = ((Boolean) map.get(attribute.name()))
                        .booleanValue() ? 1 : 0;
                break;
            }
        }
        final RowData rowData = new RowData(new byte[1024]);
        rowData.createRow(rowDef, values);
        store.writeRow(rowData);
    }
}
