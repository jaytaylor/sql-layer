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
package com.foundationdb.server.service.is;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.memoryadapter.BasicFactoryBase;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import com.foundationdb.qp.memoryadapter.SimpleMemoryGroupScan;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.ErrorCodeClass;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.monitor.CursorMonitor;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.monitor.MonitorStage;
import com.foundationdb.server.service.monitor.PreparedStatementMonitor;
import com.foundationdb.server.service.monitor.ServerMonitor;
import com.foundationdb.server.service.monitor.SessionMonitor;
import com.foundationdb.server.service.monitor.UserMonitor;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;

public class ServerSchemaTablesServiceImpl
    extends SchemaTablesService
    implements Service, ServerSchemaTablesService {

    static final TableName ERROR_CODES = new TableName (SCHEMA_NAME, "error_codes");
    static final TableName ERROR_CODE_CLASSES = new TableName (SCHEMA_NAME, "error_code_classes");
    static final TableName SERVER_INSTANCE_SUMMARY = new TableName (SCHEMA_NAME, "server_instance_summary");
    static final TableName SERVER_SERVERS = new TableName (SCHEMA_NAME, "server_servers");
    static final TableName SERVER_SESSIONS = new TableName (SCHEMA_NAME, "server_sessions");
    static final TableName SERVER_STATISTICS = new TableName (SCHEMA_NAME, "server_statistics_summary");
    static final TableName SERVER_PARAMETERS = new TableName (SCHEMA_NAME, "server_parameters");
    static final TableName SERVER_MEMORY_POOLS = new TableName (SCHEMA_NAME, "server_memory_pools");
    static final TableName SERVER_GARBAGE_COLLECTORS = new TableName (SCHEMA_NAME, "server_garbage_collectors");
    static final TableName SERVER_TAPS = new TableName (SCHEMA_NAME, "server_taps");
    static final TableName SERVER_PREPARED_STATEMENTS = new TableName (SCHEMA_NAME, "server_prepared_statements");
    static final TableName SERVER_CURSORS = new TableName (SCHEMA_NAME, "server_cursors");
    static final TableName SERVER_USERS = new TableName (SCHEMA_NAME, "server_users");

    private final MonitorService monitor;
    private final ConfigurationService configService;
    private final LayerInfoInterface serverInterface;
    private final SecurityService securityService;
    private final Store store;
    
    @Inject
    public ServerSchemaTablesServiceImpl (SchemaManager schemaManager, 
                                          MonitorService monitor, 
                                          ConfigurationService configService,
                                          LayerInfoInterface serverInterface,
                                          SecurityService securityService,
                                          Store store) {
        super(schemaManager);
        this.monitor = monitor;
        this.configService = configService;
        this.serverInterface = serverInterface;
        this.securityService = securityService;
        this.store = store;
    }

    @Override
    public void start() {
        AkibanInformationSchema ais = createTablesToRegister(schemaManager.getTypesTranslator());
        // ERROR_CODES
        attach (ais, ERROR_CODES, ServerErrorCodes.class);
        // ERROR_CODE_CLASSES
        attach (ais, ERROR_CODE_CLASSES, ServerErrorCodeClasses.class);
        //SERVER_INSTANCE_SUMMARY
        attach (ais, SERVER_INSTANCE_SUMMARY, InstanceSummary.class);
        //SERVER_SERVERS
        attach (ais, SERVER_SERVERS, Servers.class);
        //SERVER_SESSIONS
        attach (ais, SERVER_SESSIONS, Sessions.class);
        //SERVER_STATISTICS
        attach (ais, SERVER_STATISTICS, Statistics.class);
        //SERVER_PARAMETERS
        attach (ais, SERVER_PARAMETERS, ServerParameters.class);
        //SERVER_MEMORY_POOLS
        attach (ais, SERVER_MEMORY_POOLS, ServerMemoryPools.class);
        //SERVER_GARBAGE_COLLECTIONS
        attach (ais, SERVER_GARBAGE_COLLECTORS, ServerGarbageCollectors.class);
        //SERVER_TAPS
        attach (ais, SERVER_TAPS, ServerTaps.class);
        //SERVER_PREPARED_STATEMENTS
        attach (ais, SERVER_PREPARED_STATEMENTS, PreparedStatements.class);
        //SERVER_CURSORS
        attach (ais, SERVER_CURSORS, Cursors.class);
        //SERVER_USERS
        attach(ais, SERVER_USERS, Users.class);
    }

    @Override
    public void stop() {
        // nothing
    }

    @Override
    public void crash() {
        // nothing
    }
    
    protected Collection<SessionMonitor> getAccessibleSessions(Session session) {
        if (securityService.hasRestrictedAccess(session)) {
            return monitor.getSessionMonitors();
        }
        else {
            SessionMonitor sm = monitor.getSessionMonitor(session);
            if (sm == null) {
                return Collections.emptyList();
            }
            else {
                return Collections.singletonList(sm);
            }
        }
    }

    protected Collection<UserMonitor> getAccessibleUsers (Session session) {
        if (securityService.hasRestrictedAccess(session)) {
            return monitor.getUserMonitors();
        } else {
            UserMonitor um = monitor.getUserMonitor(session);
            if (um == null) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(um);
            }
        }
    }
    
    private class InstanceSummary extends BasicFactoryBase {

        public InstanceSummary(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }
        
        @Override
        public long rowCount(Session session) {
            return 1L;
        }
        
        private class Scan extends BaseScan {
            
            public Scan (Session session, RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                String hostName = null;
                try {
                    hostName = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    // do nothing -> Can't get the local host name/ip address
                    // return null as a host name
                }
                
                Long compile_time = ManagementFactory.getCompilationMXBean().getTotalCompilationTime();
                
                return new ValuesHolderRow(rowType,
                        serverInterface.getServerName(),
                        serverInterface.getVersionInfo().versionLong,
                        hostName,
                        store.getName(),
                        compile_time,
                        ++rowCounter);
            }
        }
    }
    
    private class Servers extends BasicFactoryBase {

        public Servers(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<ServerMonitor> servers = monitor.getServerMonitors().values().iterator(); 
            return new SimpleMemoryGroupScan<ServerMonitor> (group.getAIS(), getName(), servers) {
                @Override
                protected Object[] createRow(ServerMonitor data, int hiddenPk) {
                    return new Object[] {
                            data.getServerType(),
                            (data.getLocalPort() < 0) ? null : Long.valueOf(data.getLocalPort()),
                            data.getStartTimeMillis() / 1000,
                            Long.valueOf(data.getSessionCount()),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return monitor.getServerMonitors().size();
        }

    }
    private class Statistics extends BasicFactoryBase {

        public Statistics(TableName sourceTable) {
            super (sourceTable);
        }
        
        @Override 
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }
        
        @Override
        public long rowCount(Session session) {
            return 1L;
        }
        
        private class Scan extends BaseScan {
            
            public Scan (Session session, RowType rowType) {
                super(rowType);
            }
            
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        monitor.getCount(StatementTypes.STATEMENT),
                        monitor.getCount(StatementTypes.FAILED),
                        monitor.getCount(StatementTypes.FROM_CACHE),
                        monitor.getCount(StatementTypes.LOGGED),
                        monitor.getCount(StatementTypes.CALL_STMT),
                        monitor.getCount(StatementTypes.DDL_STMT),
                        monitor.getCount(StatementTypes.DML_STMT),
                        monitor.getCount(StatementTypes.SELECT),
                        monitor.getCount(StatementTypes.OTHER_STMT),
                        ++rowCounter);
            }
        }
    }

    private class Sessions extends BasicFactoryBase {

        public Sessions(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<SessionMonitor> sessions = getAccessibleSessions(adapter.getSession()).iterator();
            return new SimpleMemoryGroupScan<SessionMonitor> (group.getAIS(), getName(), sessions) {
                @Override
                protected Object[] createRow(SessionMonitor data, int hiddenPk) {
                    MonitorStage stage = data.getCurrentStage();
                    return new Object[] {
                            (long)data.getSessionId(),
                            data.getCallerSessionId() < 0 ? null : (long)data.getCallerSessionId(),
                            data.getStartTimeMillis() / 1000,
                            data.getServerType(),
                            data.getRemoteAddress(),
                            stage == null ? null : stage.name(),
                            data.getStatementCount(),
                            data.getCurrentStatement(),
                            data.getCurrentStatementStartTimeMillis() > 0 ? data.getCurrentStatementStartTimeMillis() / 1000 : null,
                            data.getCurrentStatementEndTimeMillis()  > 0  ? data.getCurrentStatementEndTimeMillis() / 1000 : null,
                            data.getRowsProcessed() < 0 ? null : (long)data.getRowsProcessed(),
                            data.getCurrentStatementPreparedName(),
                            data.getCount(StatementTypes.FAILED),
                            data.getCount(StatementTypes.FROM_CACHE),
                            data.getCount(StatementTypes.LOGGED),
                            data.getCount(StatementTypes.CALL_STMT),
                            data.getCount(StatementTypes.DDL_STMT),
                            data.getCount(StatementTypes.DML_STMT),
                            data.getCount(StatementTypes.SELECT),
                            data.getCount(StatementTypes.OTHER_STMT),
                      hiddenPk      
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return monitor.getSessionMonitors().size();
        }
    }
    
    private class ServerErrorCodes extends BasicFactoryBase {

        public ServerErrorCodes(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<ErrorCode> errorCodes = Iterators.forArray(ErrorCode.values());
            return new SimpleMemoryGroupScan<ErrorCode> (group.getAIS(), getName(), errorCodes) {
                @Override
                protected Object[] createRow(ErrorCode data, int hiddenPk) {
                    return new Object[] {
                            data.getFormattedValue(),
                            data.name(),
                            data.getMessage(),
                            null,
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return ErrorCode.values().length;
        }
    }

    private class ServerErrorCodeClasses extends BasicFactoryBase {

        public ServerErrorCodeClasses(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<ErrorCodeClass> errorCodes = ErrorCodeClass.getClasses().iterator();
            return new SimpleMemoryGroupScan<ErrorCodeClass> (group.getAIS(), getName(), errorCodes) {
                @Override
                protected Object[] createRow(ErrorCodeClass data, int hiddenPk) {
                    return new Object[] {
                            data.getKey(),
                            data.getDescription(),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return ErrorCodeClass.getClasses().size();
        }
    }

    private class ServerParameters extends BasicFactoryBase {
        public ServerParameters(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<Map.Entry<String,String>> properties = configService.getProperties().entrySet().iterator();
            return new SimpleMemoryGroupScan<Map.Entry<String,String>> (group.getAIS(), getName(), properties) {
                @Override
                protected Object[] createRow(Map.Entry<String,String> data, int hiddenPk) {
                    return new Object[] {
                            data.getKey(),
                            data.getValue(),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return configService.getProperties().size();
        }
    }
    
    private class ServerMemoryPools extends BasicFactoryBase {
        public ServerMemoryPools(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<MemoryPoolMXBean> memoryPools = ManagementFactory.getMemoryPoolMXBeans().iterator();
            return new SimpleMemoryGroupScan<MemoryPoolMXBean> (group.getAIS(), getName(), memoryPools) {
                @Override
                protected Object[] createRow(MemoryPoolMXBean data, int hiddenPk) {
                    return new Object[] {
                            data.getName(),
                            data.getType().name(),
                            data.getUsage().getUsed(),
                            data.getUsage().getMax(),
                            data.getPeakUsage().getUsed(),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return ManagementFactory.getMemoryPoolMXBeans().size();
        }
    }
    
    private class ServerGarbageCollectors extends BasicFactoryBase {
        public ServerGarbageCollectors(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans().iterator();
            return new SimpleMemoryGroupScan<GarbageCollectorMXBean> (group.getAIS(), getName(), collectors) {
                @Override
                protected Object[] createRow(GarbageCollectorMXBean data, int hiddenPk) {
                    return new Object[] {
                            data.getName(),
                            data.getCollectionCount(),
                            data.getCollectionTime(),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return ManagementFactory.getGarbageCollectorMXBeans().size();
        }
    }

    private class ServerTaps extends BasicFactoryBase {
        private TapReport[] getAllReports() {
            return Tap.getReport(".*");
        }

        public ServerTaps(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<TapReport> taps = Iterators.forArray(Tap.getReport(".*"));
            return new SimpleMemoryGroupScan<TapReport> (group.getAIS(), getName(), taps) {
                @Override
                protected Object[] createRow(TapReport data, int hiddenPk) {
                    return new Object[] {
                            data.getName(),
                            data.getInCount(),
                            data.getOutCount(),
                            data.getCumulativeTime(),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return getAllReports().length;
        }
    }

    private class PreparedStatements extends BasicFactoryBase {

        public PreparedStatements(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            long total = 0;
            for (SessionMonitor smon : monitor.getSessionMonitors())
                total += smon.getPreparedStatements().size();
            return total;
        }
        
        private class Scan extends BaseScan {
            final Iterator<SessionMonitor> sessions;
            Iterator<PreparedStatementMonitor> statements = null;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                sessions = getAccessibleSessions(session).iterator();
            }

            @Override
            public Row next() {
                while ((statements == null) ||
                       !statements.hasNext()) {
                    if (!sessions.hasNext()) {
                        return null;
                    }
                    statements = sessions.next().getPreparedStatements().iterator();
                }
                PreparedStatementMonitor preparedStatement = statements.next();
                return new ValuesHolderRow(rowType,
                                              (long)preparedStatement.getSessionId(),
                                              preparedStatement.getName(),
                                              preparedStatement.getSQL(),
                                              preparedStatement.getPrepareTimeMillis()/1000,
                                              preparedStatement.getEstimatedRowCount() < 0 ? null : (long)preparedStatement.getEstimatedRowCount(),
                                              ++rowCounter);
            }
        }
    }

    private class Cursors extends BasicFactoryBase {

        public Cursors(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType( group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            long total = 0;
            for (SessionMonitor smon : monitor.getSessionMonitors())
                total += smon.getCursors().size();
            return total;
        }
        
        private class Scan extends BaseScan {
            final Iterator<SessionMonitor> sessions;
            Iterator<CursorMonitor> statements = null;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                sessions = getAccessibleSessions(session).iterator();
            }

            @Override
            public Row next() {
                while ((statements == null) ||
                       !statements.hasNext()) {
                    if (!sessions.hasNext()) {
                        return null;
                    }
                    statements = sessions.next().getCursors().iterator();
                }
                CursorMonitor cursor = statements.next();
                return new ValuesHolderRow(rowType,
                                              (long)cursor.getSessionId(),
                                              cursor.getName(),
                                              cursor.getSQL(),
                                              cursor.getPreparedStatementName(),
                                              cursor.getCreationTimeMillis()/1000,
                                              (long)cursor.getRowCount(),
                                              ++rowCounter);
            }
        }
    }

    private class Users extends BasicFactoryBase {
        public Users(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            Iterator<UserMonitor> users = getAccessibleUsers(adapter.getSession()).iterator();
            return new SimpleMemoryGroupScan<UserMonitor> (group.getAIS(), getName(), users) {
                @Override
                protected Object[] createRow(UserMonitor data, int hiddenPk) {
                    return new Object[] {
                            data.getUserName(),
                            data.getStatementCount(),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return monitor.getUserMonitors().size();
        }
    }
    
    static AkibanInformationSchema createTablesToRegister(TypesTranslator typesTranslator) {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator);
        
        builder.table(SERVER_INSTANCE_SUMMARY)
            .colString("server_name", DESCRIPTOR_MAX, false)
            .colString("server_version", DESCRIPTOR_MAX, false)
            .colString("server_host", IDENT_MAX, false)
            .colString("server_store", IDENT_MAX, false)
            .colBigInt("server_jit_compiler_time", false);
        
        builder.table(SERVER_SERVERS)
            .colString("server_type", IDENT_MAX, false)
            .colBigInt("local_port", true)
            .colSystemTimestamp("start_time", false)
            .colBigInt("session_count", true);
        
        builder.table(SERVER_STATISTICS)
            .colBigInt("query_count", false)
            .colBigInt("failed_query_count", false)
            .colBigInt("query_from_cache", false)
            .colBigInt("logged_statements", false)
            .colBigInt("call_statement_count", false)
            .colBigInt("ddl_statement_count", false)
            .colBigInt("dml_statement_count", false)
            .colBigInt("select_statement_count", false)
            .colBigInt("other_statement_count", false)
            ;
        
        builder.table(SERVER_SESSIONS)
            .colBigInt("session_id", false)
            .colBigInt("caller_session_id", true)
            .colSystemTimestamp("start_time", false)
            .colString("server_type", IDENT_MAX, false)
            .colString("remote_address", DESCRIPTOR_MAX, true)
            .colString("session_status", DESCRIPTOR_MAX, true)
            .colBigInt("query_count", false)
            .colString("last_query_executed", PATH_MAX, true)
            .colSystemTimestamp("query_start_time", true)
            .colSystemTimestamp("query_end_time", true)
            .colBigInt("query_row_count", true)
            .colString("prepared_name", IDENT_MAX, true)
            .colBigInt("failed_query_count", false)
            .colBigInt("query_from_cache", false)
            .colBigInt("logged_statements", false)

            .colBigInt("call_statement_count", false)
            .colBigInt("ddl_statement_count", false)
            .colBigInt("dml_statement_count", false)
            .colBigInt("select_statement_count", false)
            .colBigInt("other_statement_count", false)
            ;
        
        builder.table(ERROR_CODES)
            .colString("code", 5, false)
            .colString("name", DESCRIPTOR_MAX, false)
            .colString("message", IDENT_MAX, false)
            .colString("description", PATH_MAX, true);

        builder.table(ERROR_CODE_CLASSES)
            .colString("class", 2, false)
            .colString("description", PATH_MAX, true);

        builder.table(SERVER_PARAMETERS)
            .colString("parameter_name", IDENT_MAX, false)
            .colString("current_value", PATH_MAX, false);

        builder.table(SERVER_MEMORY_POOLS)
            .colString("name", IDENT_MAX, false)
            .colString("type", DESCRIPTOR_MAX, false)
            .colBigInt("used_bytes", false)
            .colBigInt("max_bytes", false)
            .colBigInt("peak_bytes", false);

        builder.table(SERVER_GARBAGE_COLLECTORS)
            .colString("name", IDENT_MAX, false)
            .colBigInt("total_count", false)
            .colBigInt("total_milliseconds", false);

        builder.table(SERVER_TAPS)
            .colString("tap_name", IDENT_MAX, false)
            .colBigInt("in_count", false)
            .colBigInt("out_count", false)
            .colBigInt("total_nanoseconds", false);

        builder.table(SERVER_PREPARED_STATEMENTS)
            .colBigInt("session_id", false)
            .colString("prepared_name", IDENT_MAX, true)
            .colString("statement", PATH_MAX, true)
            .colSystemTimestamp("prepare_time", true)
            .colBigInt("estimated_row_count", true);

        builder.table(SERVER_CURSORS)
            .colBigInt("session_id", false)
            .colString("cursor_name", IDENT_MAX, true)
            .colString("statement", PATH_MAX, true)
            .colString("prepared_name", IDENT_MAX, true)
            .colSystemTimestamp("creation_time", true)
            .colBigInt("row_count", true);

        builder.table(SERVER_USERS)
            .colString("user_name", IDENT_MAX, false)
            .colBigInt("statement_count", false);
            
        return builder.ais(false);
    }
}
