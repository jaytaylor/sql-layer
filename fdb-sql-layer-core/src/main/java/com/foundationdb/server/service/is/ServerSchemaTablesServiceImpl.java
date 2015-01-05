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
import java.util.List;
import java.util.Map;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.memoryadapter.BasicFactoryBase;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.memoryadapter.MemoryGroupCursor.GroupScan;
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
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import com.google.inject.Inject;

public class ServerSchemaTablesServiceImpl
    extends SchemaTablesService
    implements Service, ServerSchemaTablesService {

    static final TableName ERROR_CODES = new TableName (SCHEMA_NAME, "error_codes");
    static final TableName ERROR_CODE_CLASSES = new TableName (SCHEMA_NAME, "error_code_classes");
    static final TableName SERVER_INSTANCE_SUMMARY = new TableName (SCHEMA_NAME, "server_instance_summary");
    static final TableName SERVER_SERVERS = new TableName (SCHEMA_NAME, "server_servers");
    static final TableName SERVER_SESSIONS = new TableName (SCHEMA_NAME, "server_sessions");
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
            return 1;
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
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return monitor.getServerMonitors().size();
        }
        
        private class Scan extends BaseScan {
            final Iterator<ServerMonitor> servers = monitor.getServerMonitors().values().iterator(); 
            public Scan(Session session, RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (!servers.hasNext()) {
                    return null;
                }
                ServerMonitor server = servers.next();
                return new ValuesHolderRow(rowType,
                                              server.getServerType(),
                                              (server.getLocalPort() < 0) ? null : Long.valueOf(server.getLocalPort()),
                                              server.getStartTimeMillis()/1000,
                                              Long.valueOf(server.getSessionCount()),
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
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return monitor.getSessionMonitors().size();
        }
        
        private class Scan extends BaseScan {
            final Iterator<SessionMonitor> sessions;
            public Scan(Session session, RowType rowType) {
                super(rowType);
                sessions = getAccessibleSessions(session).iterator();
            }

            @Override
            public Row next() {
                if (!sessions.hasNext()) {
                    return null;
                }
                SessionMonitor session = sessions.next();
                MonitorStage stage = session.getCurrentStage();
                return new ValuesHolderRow(rowType,
                                              (long)session.getSessionId(),
                                              session.getCallerSessionId() < 0 ? null : (long)session.getCallerSessionId(),
                                              (int)(session.getStartTimeMillis()/1000),
                                              session.getServerType(),
                                              session.getRemoteAddress(),
                                              (stage == null) ? null : stage.name(),
                                              (long)session.getStatementCount(),
                                              session.getCurrentStatement(),
                                              session.getCurrentStatementStartTimeMillis() > 0 ? (int)(session.getCurrentStatementStartTimeMillis() / 1000) : null,
                                              session.getCurrentStatementEndTimeMillis() > 0 ? (int)(session.getCurrentStatementEndTimeMillis()/1000) : null,
                                              session.getRowsProcessed() < 0 ? null : (long)session.getRowsProcessed(),
                                              session.getCurrentStatementPreparedName(),
                                              ++rowCounter);
            }
        }
    }
    
    private class ServerErrorCodes extends BasicFactoryBase {

        public ServerErrorCodes(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return ErrorCode.values().length;
        }
        
        private class Scan extends BaseScan {

            private final ErrorCode[] codes = ErrorCode.values();
            public Scan(Session session, RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (rowCounter >= codes.length)
                    return null;
                return new ValuesHolderRow(rowType,
                        codes[(int)rowCounter].getFormattedValue(),
                        codes[(int)rowCounter].name(),
                        codes[(int)rowCounter].getMessage(),
                        null,
                        ++rowCounter);
            }
        }
    }

    private class ServerErrorCodeClasses extends BasicFactoryBase {

        public ServerErrorCodeClasses(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return ErrorCodeClass.getClasses().size();
        }

        private class Scan extends BaseScan {

            private final List<ErrorCodeClass> classes = ErrorCodeClass.getClasses();
            public Scan(Session session, RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (rowCounter >= classes.size())
                    return null;
                return new ValuesHolderRow(rowType,
                        classes.get((int)rowCounter).getKey(),
                        classes.get((int)rowCounter).getDescription(),
                        ++rowCounter);
            }
        }
    }

    private class ServerParameters extends BasicFactoryBase {
        public ServerParameters(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return configService.getProperties().size();
        }

        private class Scan extends BaseScan {
            private Iterator<Map.Entry<String,String>> propertyIt;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                propertyIt = configService.getProperties().entrySet().iterator();
            }

            @Override
            public Row next() {
                if (!propertyIt.hasNext())
                    return null;
                Map.Entry<String,String> prop = propertyIt.next();
                return new ValuesHolderRow(rowType,
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
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return ManagementFactory.getMemoryPoolMXBeans().size();
        }

        private class Scan extends BaseScan {
            private final Iterator<MemoryPoolMXBean> it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                it = ManagementFactory.getMemoryPoolMXBeans().iterator();
            }

            @Override
            public Row next() {
                if(!it.hasNext()) {
                    return null;
                }
                MemoryPoolMXBean pool = it.next();
                return new ValuesHolderRow(rowType,
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
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return ManagementFactory.getGarbageCollectorMXBeans().size();
        }

        private class Scan extends BaseScan {
            private final Iterator<GarbageCollectorMXBean> it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                it = ManagementFactory.getGarbageCollectorMXBeans().iterator();
            }

            @Override
            public Row next() {
                if(!it.hasNext()) {
                    return null;
                }
                GarbageCollectorMXBean pool = it.next();
                return new ValuesHolderRow(rowType,
                                      pool.getName(),
                                      pool.getCollectionCount(),
                                      pool.getCollectionTime(),
                                      ++rowCounter);
            }
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
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return getAllReports().length;
        }

        private class Scan extends BaseScan {
            private final TapReport[] reports;
            private int it = 0;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                reports = getAllReports();
            }

            @Override
            public Row next() {
                if(it >= reports.length) {
                    return null;
                }
                TapReport report = reports[it++];
                return new ValuesHolderRow(rowType,
                                      report.getName(),
                                      report.getInCount(),
                                      report.getOutCount(),
                                      report.getCumulativeTime(),
                                      ++rowCounter);
            }
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
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return monitor.getUserMonitors().size();
        }
        
        private class Scan extends BaseScan {
            final Iterator<UserMonitor> users;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                users = getAccessibleUsers(session).iterator();
            }

            @Override
            public Row next() {
                if (!users.hasNext()) {
                    return null;
                }
                UserMonitor user = users.next();
                return new ValuesHolderRow(rowType,
                                            user.getUserName(),
                                            user.getStatementCount(),
                                            ++rowCounter);
            }
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
            .colString("prepared_name", IDENT_MAX, true);
        
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
