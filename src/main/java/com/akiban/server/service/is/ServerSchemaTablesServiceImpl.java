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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.Iterator;
import java.util.Map;

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
import com.akiban.server.service.monitor.MonitorService;
import com.akiban.server.service.monitor.MonitorStage;
import com.akiban.server.service.monitor.ServerMonitor;
import com.akiban.server.service.monitor.SessionMonitor;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.google.inject.Inject;

public class ServerSchemaTablesServiceImpl
    extends SchemaTablesService
    implements Service, ServerSchemaTablesService {

    static final TableName ERROR_CODES = new TableName (SCHEMA_NAME, "error_codes");
    static final TableName SERVER_INSTANCE_SUMMARY = new TableName (SCHEMA_NAME, "server_instance_summary");
    static final TableName SERVER_SERVERS = new TableName (SCHEMA_NAME, "server_servers");
    static final TableName SERVER_SESSIONS = new TableName (SCHEMA_NAME, "server_sessions");
    static final TableName SERVER_PARAMETERS = new TableName (SCHEMA_NAME, "server_parameters");
    static final TableName SERVER_MEMORY_POOLS = new TableName (SCHEMA_NAME, "server_memory_pools");
    static final TableName SERVER_GARBAGE_COLLECTORS = new TableName (SCHEMA_NAME, "server_garbage_collectors");
    
    private final MonitorService monitor;
    private final ConfigurationService configService;
    private final AkServerInterface serverInterface;
    
    @Inject
    public ServerSchemaTablesServiceImpl (SchemaManager schemaManager, 
                                          MonitorService monitor, 
                                          ConfigurationService configService,
                                          AkServerInterface serverInterface) {
        super(schemaManager);
        this.monitor = monitor;
        this.configService = configService;
        this.serverInterface = serverInterface;
    }

    @Override
    public void start() {
        AkibanInformationSchema ais = createTablesToRegister();
        // ERROR_CODES
        attach (ais, true, ERROR_CODES, ServerErrorCodes.class);
        //SERVER_INSTANCE_SUMMARY
        attach (ais, true, SERVER_INSTANCE_SUMMARY, InstanceSummary.class);
        //SERVER_SERVERS
        attach (ais, true, SERVER_SERVERS, Servers.class);
        //SERVER_SESSIONS
        attach (ais, true, SERVER_SESSIONS, Sessions.class);
        //SERVER_PARAMETERS
        attach (ais, true, SERVER_PARAMETERS, ServerParameters.class);
        //SERVER_MEMORY_POOLS
        attach (ais, true, SERVER_MEMORY_POOLS, ServerMemoryPools.class);
        //SERVER_GARBAGE_COLLECTIONS
        attach (ais, true, SERVER_GARBAGE_COLLECTORS, ServerGarbageCollectors.class);
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
                ValuesRow row = new ValuesRow (rowType,
                        serverInterface.getServerName(),
                        serverInterface.getServerVersion(),
                        ++rowCounter);
                return row;
            }
        }
    }
    
    private class Servers extends BasicFactoryBase {

        public Servers(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return monitor.getServerMonitors().size();
        }
        
        private class Scan extends BaseScan {
            final Iterator<ServerMonitor> servers = monitor.getServerMonitors().values().iterator(); 
            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (!servers.hasNext()) {
                    return null;
                }
                ServerMonitor server = servers.next();
                ValuesRow row = new ValuesRow(rowType,
                                              server.getServerType(),
                                              (server.getLocalPort() < 0) ? null : Long.valueOf(server.getLocalPort()),
                                              null, // see below
                                              Long.valueOf(server.getSessionCount()),
                                              ++rowCounter);
                ((FromObjectValueSource)row.eval(2)).setExplicitly(server.getStartTimeMillis()/1000, AkType.TIMESTAMP);
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
            return monitor.getSessionMonitors().size();
        }
        
        private class Scan extends BaseScan {
            final Iterator<SessionMonitor> sessions = monitor.getSessionMonitors().iterator(); 
            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (!sessions.hasNext()) {
                    return null;
                }
                SessionMonitor session = sessions.next();
                MonitorStage stage = session.getCurrentStage();
                ValuesRow row = new ValuesRow(rowType,
                                              session.getSessionId(),
                                              session.getCallerSessionId() < 0 ? null : session.getCallerSessionId(),
                                              null, // see below
                                              session.getServerType(),
                                              session.getRemoteAddress(),
                                              (stage == null) ? null : stage.name(),
                                              session.getStatementCount(),
                                              session.getCurrentStatement(),
                                              null, null,
                                              ++rowCounter);
                ((FromObjectValueSource)row.eval(2)).setExplicitly(session.getStartTimeMillis()/1000, AkType.TIMESTAMP);
                long queryStartTime = session.getCurrentStatementStartTimeMillis();
                if (queryStartTime >= 0)
                    ((FromObjectValueSource)row.eval(8)).setExplicitly(queryStartTime/1000, AkType.TIMESTAMP);
                long queryEndTime = session.getCurrentStatementEndTimeMillis();
                if (queryEndTime >= 0)
                    ((FromObjectValueSource)row.eval(9)).setExplicitly(queryEndTime/1000, AkType.TIMESTAMP);
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
            private Iterator<Map.Entry<String,String>> propertyIt;

            public Scan(RowType rowType) {
                super(rowType);
                propertyIt = configService.getProperties().entrySet().iterator();
            }

            @Override
            public Row next() {
                if (!propertyIt.hasNext())
                    return null;
                Map.Entry<String,String> prop = propertyIt.next();
                return new ValuesRow (rowType,
                                      prop.getKey(),
                                      prop.getValue(),
                                      ++rowCounter);
            }
        }
    }
    
    private class ServerMemoryPools extends BasicFactoryBase {
        public ServerMemoryPools(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return ManagementFactory.getMemoryPoolMXBeans().size();
        }

        private class Scan extends BaseScan {
            private final Iterator<MemoryPoolMXBean> it;

            public Scan(RowType rowType) {
                super(rowType);
                it = ManagementFactory.getMemoryPoolMXBeans().iterator();
            }

            @Override
            public Row next() {
                if(!it.hasNext()) {
                    return null;
                }
                MemoryPoolMXBean pool = it.next();
                return new ValuesRow (rowType,
                                      pool.getName(),
                                      pool.getType().name(),
                                      pool.getUsage().getUsed(),
                                      pool.getUsage().getMax(),
                                      pool.getPeakUsage().getUsed(),
                                      ++rowCounter);
            }
        }
    }
    
    private class ServerGarbageCollectors extends BasicFactoryBase {
        public ServerGarbageCollectors(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return ManagementFactory.getGarbageCollectorMXBeans().size();
        }

        private class Scan extends BaseScan {
            private final Iterator<GarbageCollectorMXBean> it;

            public Scan(RowType rowType) {
                super(rowType);
                it = ManagementFactory.getGarbageCollectorMXBeans().iterator();
            }

            @Override
            public Row next() {
                if(!it.hasNext()) {
                    return null;
                }
                GarbageCollectorMXBean pool = it.next();
                return new ValuesRow (rowType,
                                      pool.getName(),
                                      pool.getCollectionCount(),
                                      pool.getCollectionTime(),
                                      ++rowCounter);
            }
        }
    }

    static AkibanInformationSchema createTablesToRegister() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        
        builder.userTable(SERVER_INSTANCE_SUMMARY)
            .colString("server_name", DESCRIPTOR_MAX, false)
            .colString("server_version", DESCRIPTOR_MAX, false);
        
        builder.userTable(SERVER_SERVERS)
            .colString("server_type", IDENT_MAX, false)
            .colBigInt("local_port", true)
            .colTimestamp("start_time", false)
            .colBigInt("session_count", true);
        
        builder.userTable(SERVER_SESSIONS)
            .colBigInt("session_id", false)
            .colBigInt("caller_session_id", true)
            .colTimestamp("start_time", false)
            .colString("server_type", IDENT_MAX, false)
            .colString("remote_address", DESCRIPTOR_MAX, true)
            .colString("session_status", DESCRIPTOR_MAX, true)
            .colBigInt("query_count", false)
            .colString("last_query_executed", PATH_MAX, true)
            .colTimestamp("query_start_time", true)
            .colTimestamp("query_end_time", true);
        
        builder.userTable(ERROR_CODES)
            .colString("code", 5, false)
            .colString("name", DESCRIPTOR_MAX, false)
            .colString("message", IDENT_MAX, false)
            .colString("description", PATH_MAX, true);

        builder.userTable(SERVER_PARAMETERS)
            .colString("parameter_name", IDENT_MAX, false)
            .colString("current_value", PATH_MAX, false);

        builder.userTable(SERVER_MEMORY_POOLS)
            .colString("name", IDENT_MAX, false)
            .colString("type", DESCRIPTOR_MAX, false)
            .colBigInt("used_bytes", false)
            .colBigInt("max_bytes", false)
            .colBigInt("peak_bytes", false);

        builder.userTable(SERVER_GARBAGE_COLLECTORS)
            .colString("name", IDENT_MAX, false)
            .colBigInt("total_count", false)
            .colBigInt("total_milliseconds", false);

        return builder.ais(false);
    }
}
