/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.sql.pg;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.akiban.sql.optimizer.plan.CostEstimate;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.ConnectionTerminatedException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.UnsupportedConfigurationException;
import com.akiban.sql.parser.AlterServerNode;

public class PostgresServerStatement implements PostgresStatement {
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
    public void sendDescription(PostgresQueryContext context, boolean always)
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.PARAMETER_DESCRIPTION_TYPE.code());
            messenger.writeShort(0);
            messenger.sendMessage();
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        
        context.checkQueryCancelation();
        PostgresServerSession server = context.getServer();
        try {
            doOperation(server);
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
        return 0;
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

    protected void doOperation (PostgresServerSession session) throws Exception {
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
            setVariable (session, statement.getVariable(), statement.getValue());
            sendComplete (session.getMessenger());
            break;
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
            sendComplete (session.getMessenger());
            break;
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
                sendComplete(session.getMessenger());
            break;
        case SHUTDOWN:
            {
                String msg = "Akiban server being shutdown";
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
            // Note: no command completion, since always terminating.
            break;
        }
        // Now finally do what was indicated for the current connection.
        if (completeCurrent != null)
            throw new ConnectionTerminatedException(completeCurrent);
    }

    protected void setVariable(PostgresServerSession server, String variable, String value) {
        if (!Arrays.asList(PostgresSessionStatement.ALLOWED_CONFIGURATION).contains(variable))
            throw new UnsupportedConfigurationException (variable);
        server.setProperty(variable, value);
    }
    
    protected void sendComplete (PostgresMessenger messenger) throws IOException {
        messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
        messenger.writeString(statement.statementToString());
        messenger.sendMessage();
        
    }

    protected void shutdown () throws Exception {
        new Thread() {
            @Override
            public void run() {
                try {
                    MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
                    jmxServer.invoke(new ObjectName ("com.akiban:type=SHUTDOWN"), "shutdown", new Object[0], new String[0]);
                }
                catch (Exception ex) {
                    LOG.error("Shutdown failed", ex);
                }
            }
        }.start();
    }
}
