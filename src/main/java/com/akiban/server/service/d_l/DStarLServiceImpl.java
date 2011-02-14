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

package com.akiban.server.service.d_l;

import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DDLFunctionsImpl;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.DMLFunctionsImpl;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicReference;

public class DStarLServiceImpl implements DStarLService, Service<DStarLService>, JmxManageable {

    private final DDLFunctions ddlFunctions = new DDLFunctionsImpl();
    private final DMLFunctions dmlFunctions = new DMLFunctionsImpl(ddlFunctions);
    private final AtomicReference<String> usingSchema = new AtomicReference<String>("test");

    private final DStarLMXBean bean = new DStarLMXBean() {
        @Override
        public String getUsingSchema() {
            return usingSchema.get();
        }

        @Override
        public void setUsingSchema(String schema) {
            usingSchema.set(schema);
        }

        public void createTable(String schema, String ddl) {
            try {
                ddlFunctions.createTable(new SessionImpl(), schema, ddl);
            } catch (InvalidOperationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void createTable(String ddl) {
            createTable(usingSchema.get(), ddl);
        }

        @Override
        public void dropTable(String schema, String tableName) {
            try {
                ddlFunctions.dropTable(new SessionImpl(), new TableName(schema, tableName));
            } catch (InvalidOperationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void dropTable(String tableName) {
            dropTable(usingSchema.get(), tableName);
        }

        @Override
        public void dropGroup(String groupName) {
            try {
                ddlFunctions.dropGroup(new SessionImpl(), groupName);
            } catch (InvalidOperationException e) {
                throw new RuntimeException(e);
            }
        }

        public void writeRow(String schema, String table, String fields) {
            try {
                final Session session = new SessionImpl();
                int tableId = ddlFunctions.getTableId(session, new TableName(schema, table));
                NewRow row = new NiceRow(tableId);
                String[] fieldsArray = fields.split(",\\s*");
                for (int i=0; i < fieldsArray.length; ++i) {
                    String field = java.net.URLDecoder.decode(fieldsArray[i], "UTF-8");
                    row.put(i, field);
                }
                dmlFunctions.writeRow(session, row);
            } catch (InvalidOperationException e) {
                throw new RuntimeException(e.getMessage());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        @Override
        public void writeRow(String table, String fields) {
            writeRow(usingSchema.get(), table, fields);
        }
    };

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("DStarL", bean, DStarLMXBean.class);
    }

    @Override
    public DStarLService cast() {
        return this;
    }

    @Override
    public Class<DStarLService> castClass() {
        return DStarLService.class;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }
}
