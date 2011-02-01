package com.akiban.cserver.service.d_l;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.DDLFunctions;
import com.akiban.cserver.api.DDLFunctionsImpl;
import com.akiban.cserver.api.DMLFunctions;
import com.akiban.cserver.api.DMLFunctionsImpl;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicReference;

public class DStarLServiceImpl implements DStarLService, Service<DStarLService>, JmxManageable {

    private final DDLFunctions ddlFunctions;
    private final DMLFunctions dmlFunctions;
    private final AtomicReference<String> usingTable = new AtomicReference<String>("test");

    public DStarLServiceImpl() {
        DDLFunctionsImpl ddl = new DDLFunctionsImpl();
        ddlFunctions = ddl;
        dmlFunctions = new DMLFunctionsImpl(ddl);
    }

    private final DStarLMXBean bean = new DStarLMXBean() {
        @Override
        public String getUsingSchema() {
            return usingTable.get();
        }

        @Override
        public void setUsingSchema(String schema) {
            usingTable.set(schema);
        }

        @Override
        public void createTable(String schema, String ddl) {
            try {
                ddlFunctions.createTable(new SessionImpl(), schema, ddl);
            } catch (InvalidOperationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void createTable(String ddl) {
            createTable(usingTable.get(), ddl);
        }

        @Override
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
            writeRow(usingTable.get(), table, fields);
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
