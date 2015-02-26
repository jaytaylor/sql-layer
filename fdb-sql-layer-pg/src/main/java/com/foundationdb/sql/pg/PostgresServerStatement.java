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
package com.foundationdb.sql.pg;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.foundationdb.sql.optimizer.plan.CostEstimate;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.ConnectionTerminatedException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.SecurityException;
import com.foundationdb.server.error.UnsupportedConfigurationException;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.sql.parser.AlterServerNode;

public class PostgresServerStatement extends PostgresStatementResults
                                     implements PostgresStatement {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresServerStatement.class);
    private final AlterServerNode statement;
    private long aisGeneration;

    protected PostgresServerStatement (AlterServerNode stmt) {
        this.statement = stmt;
    }
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            if (params) {
                messenger.beginMessage(PostgresMessages.PARAMETER_DESCRIPTION_TYPE.code());
                messenger.writeShort(0);
                messenger.sendMessage();
            }
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        
        context.checkQueryCancelation();
        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.OTHER_STMT);
        try {
            return doOperation(server);
        } catch (Exception e) {
            if (!(e instanceof ConnectionTerminatedException))
                LOG.error("Execute command failed: " + e.getMessage());
            if (e instanceof IOException)
                throw (IOException)e;
            else if (e instanceof InvalidOperationException) {
                throw (InvalidOperationException)e;
            }
            throw new AkibanInternalException ("PostgrsServerStatement execute failed.", e);
        }
    }

    @Override
    public boolean hasAISGeneration() {
        return aisGeneration != 0;
    }

    @Override
    public void setAISGeneration(long aisGeneration) {
        this.aisGeneration = aisGeneration;
    }

    @Override
    public long getAISGeneration() {
        return aisGeneration;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        return this;
    }

    @Override
    public boolean putInCache() {
        return false;
    }

    @Override
    public CostEstimate getCostEstimate() {
        return null;
    }

    protected PostgresStatementResult doOperation (PostgresServerSession session) throws Exception {
        if (!session.getSecurityService().hasRestrictedAccess(session.getSession()))
            throw new SecurityException("Operation not allowed");
        PostgresServerConnection current = (PostgresServerConnection)session;
        PostgresServer server = current.getServer();
        Integer sessionId = statement.getSessionID();
        String byUser = session.getProperty("user");
        String completeCurrent = null;
        /*
         * Note: Caution when adding new types and check execution under ROLLBACK, see getTransactionAbortedMode()
         */
        switch (statement.getAlterSessionType()) {
        case SET_SERVER_VARIABLE:
            setVariable (server, statement.getVariable(), statement.getValue());
            return statementComplete(statement);
        case INTERRUPT_SESSION:
            if (sessionId == null) {
                for (PostgresServerConnection conn : server.getConnections()) {
                    if (conn != current)
                        conn.cancelQuery(null, byUser);
                }
            } 
            else {
                PostgresServerConnection conn = server.getConnection(sessionId);
                if ((conn != null) && (conn != current))
                    conn.cancelQuery(null, byUser);
            }
            return statementComplete(statement);
        case DISCONNECT_SESSION:
        case KILL_SESSION:
            {
                String msg = "your session being disconnected";
                if (sessionId == null) {
                    Collection<PostgresServerConnection> conns = server.getConnections();
                    for (PostgresServerConnection conn : conns) {
                        if (conn == current)
                            completeCurrent = msg;
                        else
                            conn.cancelQuery(msg, byUser);
                    }
                    for (PostgresServerConnection conn : conns) {
                        if (conn != current)
                            conn.waitAndStop();
                    }
                }
                else {
                    PostgresServerConnection conn = server.getConnection(sessionId);
                    if (conn == current)
                        completeCurrent = msg;
                    else if (conn != null) {
                        conn.cancelQuery(msg, byUser);
                        conn.waitAndStop();
                    }
                    // TODO: Else no such session error?
                }
            }
            if (completeCurrent == null)
                return statementComplete(statement);
            else
                throw new ConnectionTerminatedException(completeCurrent);
        case SHUTDOWN:
            {
                String msg = "FoundationDB SQL Layer being shutdown";
                Collection<PostgresServerConnection> conns = server.getConnections();
                for (PostgresServerConnection conn : conns) {
                    if (conn == current)
                        completeCurrent = msg;
                    else if (!statement.isShutdownImmediate())
                        conn.cancelQuery(msg, byUser);
                }
                if (!statement.isShutdownImmediate()) {
                    for (PostgresServerConnection conn : conns) {
                        if (conn != current)
                            conn.waitAndStop();
                    }
                }
                shutdown();
            }
            if (completeCurrent != null)
                throw new ConnectionTerminatedException(completeCurrent);
            else
                // Note: no command completion, since always terminating.
                return noResult();
        default:
            throw new IllegalArgumentException(statement.toString());
        }
    }

    protected void setVariable(PostgresServer server, String variable, String value) {
        String cased = PostgresSessionStatement.allowedConfiguration(variable);
        if (cased == null)
            throw new UnsupportedConfigurationException (variable);
        
        if (cased == "statementCacheCapacity") {
            String capacityString = value;
            if (value == null) {
                //fdbsql.postgres.statementCacheCapacity=0
                capacityString = server.getProperties().getProperty("statementCacheCapacity");
            }
            int capacity = Integer.parseInt(capacityString);
            server.setStatementCacheCapacity(capacity);
            LOG.error("Setting statementCacheCapacity {}", capacity);
        } else if (cased == "resetStatementCache") {
            server.resetStatementCache();
            LOG.error("resetStatementCache");
        } else {
            if (value == null)
                server.getProperties().remove(variable);
            else
                server.getProperties().setProperty(variable, value);
            for (PostgresServerConnection conn : server.getConnections()) {
                if (!conn.getProperties().containsKey(variable)) {
                    conn.setProperty(variable, null); // As though SET x TO DEFAULT.
                }
            }
        }
    }

    protected void shutdown () throws Exception {
        new Thread() {
            @Override
            public void run() {
                try {
                    MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
                    jmxServer.invoke(new ObjectName ("com.foundationdb:type=SHUTDOWN"), "shutdown", new Object[0], new String[0]);
                }
                catch (Exception ex) {
                    LOG.error("Shutdown failed", ex);
                }
            }
        }.start();
    }
}
