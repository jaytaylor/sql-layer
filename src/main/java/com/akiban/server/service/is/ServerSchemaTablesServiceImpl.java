/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
package com.akiban.server.service.is;

import java.util.Iterator;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.memoryadapter.BasicFactoryBase;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.AkServerInterface;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.instrumentation.Event;
import com.akiban.server.service.instrumentation.SessionTracer;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.sql.pg.PostgresService;
import com.google.inject.Inject;

public class ServerSchemaTablesServiceImpl
    extends SchemaTablesService
    implements Service<ServerSchemaTablesService>, ServerSchemaTablesService {

    static final TableName ERROR_CODES = new TableName (SCHEMA_NAME, "error_codes");
    static final TableName SERVER_INSTANCE_SUMMARY = new TableName (SCHEMA_NAME, "server_instance_summary");
    static final TableName SERVER_SESSIONS = new TableName (SCHEMA_NAME, "server_sessions");
    static final TableName SERVER_PARAMETERS = new TableName (SCHEMA_NAME, "server_parameters");
    
    private final PostgresService manager;
    private final ConfigurationService configService;
    private final AkServerInterface serverInterface;
    
    @Inject
    public ServerSchemaTablesServiceImpl (SchemaManager schemaManager, PostgresService manager, 
            ConfigurationService configService,
            AkServerInterface serverInterface) {
        super(schemaManager);
        this.manager = manager;
        this.configService = configService;
        this.serverInterface = serverInterface;
    }
    
    @Override
    public ServerSchemaTablesService cast() {
        return this;
    }

    @Override
    public Class<ServerSchemaTablesService> castClass() {
        return ServerSchemaTablesService.class;
    }

    @Override
    public void start() {
        AkibanInformationSchema ais = createTablesToRegister();
        // ERROR_CODES
        attach (ais, true, ERROR_CODES, ServerErrorCodes.class);
        //SERVER_INSTANCE_SUMMARY
        attach (ais, true, SERVER_INSTANCE_SUMMARY, InstanceSummary.class);
        //SERVER_SESSIONS
        attach (ais, true, SERVER_SESSIONS, Sessions.class);
        //SERVER_PARAMETERS
        attach (ais, true, SERVER_PARAMETERS, ServerParameters.class);
    }

    @Override
    public void stop() {
        // nothing
    }

    @Override
    public void crash() {
        // nothing
    }
    
    private class InstanceSummary extends BasicFactoryBase {

        public InstanceSummary(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
        private class Scan extends BaseScan {
            
            public Scan (RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                long startTime = System.currentTimeMillis() -  
                        (manager.getServer().getUptime() / 1000000);
                ValuesRow row = new ValuesRow (rowType,
                        serverInterface.getServerName(),
                        serverInterface.getServerVersion(),
                        manager.getServer().isListening() ? "RUNNING" : "CLOSED",
                        startTime,
                        ++rowCounter);
                ((FromObjectValueSource)row.eval(3)).setExplicitly(startTime/1000, AkType.TIMESTAMP);

                return row;
            }
        }
    }
    
    private class Sessions extends BasicFactoryBase {

        public Sessions(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return manager.getServer().getCurrentSessions().size();
        }
        
        private class Scan extends BaseScan {
            final Iterator<Integer> sessions = manager.getServer().getCurrentSessions().iterator(); 
            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (!sessions.hasNext()) {
                    return null;
                }
                int sessionID = 0;
                do {
                    sessionID = sessions.next();
                } while (manager.getServer().getConnection(sessionID) == null);
                
                SessionTracer trace = manager.getServer().getConnection(sessionID).getSessionTracer();
                String eventName = null;
                if (trace.getCurrentEvents().length > 0) {
                    Event event = (Event)trace.getCurrentEvents()[0];
                    eventName = event.getName();
                }
                        
                ValuesRow row = new ValuesRow (rowType,
                        sessionID,
                        trace.getStartTime().getTime(),
                        boolResult(trace.isEnabled()),
                        eventName,
                        trace.getRemoteAddress(),
                        trace.getCurrentStatement(),
                        ++rowCounter);
                ((FromObjectValueSource)row.eval(1)).setExplicitly(trace.getStartTime().getTime()/1000, AkType.TIMESTAMP);
                return row;
            }
        }
    }
    
    private class ServerErrorCodes extends BasicFactoryBase {

        public ServerErrorCodes(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return ErrorCode.values().length;
        }
        
        private class Scan extends BaseScan {

            private final ErrorCode[] codes = ErrorCode.values();
            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (rowCounter >= codes.length)
                    return null;
                return new ValuesRow (rowType,
                        codes[rowCounter].getFormattedValue(),
                        codes[rowCounter].name(),
                        codes[rowCounter].getMessage(),
                        null,
                        ++rowCounter);
            }
        }
    }

    private class ServerParameters extends BasicFactoryBase {
        public ServerParameters(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return configService.getProperties().size();
        }

        private class Scan extends BaseScan {
            private Iterator<Property> propertyIt;

            public Scan(RowType rowType) {
                super(rowType);
                propertyIt = configService.getProperties().iterator();
            }

            @Override
            public Row next() {
                if (!propertyIt.hasNext())
                    return null;
                Property prop = propertyIt.next();
                return new ValuesRow (rowType,
                                      prop.getKey(),
                                      prop.getValue(),
                                      ++rowCounter);
            }
        }
    }
    
    static AkibanInformationSchema createTablesToRegister() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        
        builder.userTable(SERVER_INSTANCE_SUMMARY)
            .colString("server_name", DESCRIPTOR_MAX, false)
            .colString("server_version", DESCRIPTOR_MAX, false)
            .colString("instance_status", DESCRIPTOR_MAX, false)
            .colTimestamp("start_time");
        
        builder.userTable(SERVER_SESSIONS)
            .colBigInt("session_id", false)
            .colTimestamp("start_time", false)
            .colString("instrumentation_enabled", YES_NO_MAX, false)
            .colString("session_status", DESCRIPTOR_MAX, true)
            .colString("remote_address", DESCRIPTOR_MAX, true)
            .colString("last_sql_executed", PATH_MAX, true);
        
        builder.userTable(ERROR_CODES)
            .colString("code", 5, false)
            .colString("name", DESCRIPTOR_MAX, false)
            .colString("message", IDENT_MAX, false)
            .colString("description", PATH_MAX, true);

        builder.userTable(SERVER_PARAMETERS)
            .colString("parameter_name", IDENT_MAX, false)
            .colString("current_value", PATH_MAX, false);

        return builder.ais(false);
    }
}
