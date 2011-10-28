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

import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Target;
import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.Store;
import com.persistit.exception.PersistitException;

/**
 * Implements the Target interface. This class is intended only for unit tests.
 * In the normal system life-cyle the AIS data will be inserted through normal
 * SQL calls to the MySQL head which will in turn sets WriteRowRequest messages.
 * 
 * @author peter
 * 
 */
public class AkServerAisTarget extends Target {

    private final Store store;
    
    private final Session session;

    // Target interface

    public void deleteAll() {
        deleteTable(type);
        deleteTable(group);
        deleteTable(table);
        deleteTable(column);
        deleteTable(join);
        deleteTable(joinColumn);
        deleteTable(index);
        deleteTable(indexColumn);
    }

    private void deleteTable(final String name) {
        final ModelObject modelObject = MetaModel.only().definition(name);
        final RowDef rowDef = store.getRowDefCache().getRowDef(modelObject.tableName());
        if (rowDef == null) {
            throw new IllegalStateException(
                    "Missing table definition for AIS table " + name);
        }
        try {
        store.truncateGroup(session, rowDef.getRowDefId());
        } catch (PersistitException ex) {
            throw new PersistItErrorException (ex);
        }
    }

    @Override
    public void writeCount(int count) {
    }

    public void close() {
        session.close();
    }

    public AkServerAisTarget(final Store store, SessionService sessionService) {
        this.store = store;
        this.session = sessionService.createSession();
    }

    // For use by this class

    @Override
    public void writeVersion(int modelVersion)
    {
        //no writing version for test classes. 
    }
    @Override
    protected final void write(String typename, Map<String, Object> map) {
        ModelObject modelObject = MetaModel.only().definition(typename);
        final RowDef rowDef = store.getRowDefCache().getRowDef(modelObject.tableName());
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
        try {
            store.writeRow(session, rowData);
        } catch (PersistitException ex) {
            throw new PersistItErrorException (ex);
        }
    }
}
