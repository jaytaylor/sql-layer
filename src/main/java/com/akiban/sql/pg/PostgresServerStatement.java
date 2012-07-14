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
package com.akiban.sql.pg;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context, boolean always)
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows,
            boolean usePVals) throws IOException {
        
        context.checkQueryCancelation();
        PostgresServerSession server = context.getServer();
        try {
            doOperation(server);
        } catch (Exception e) {
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
    
    protected void doOperation (PostgresServerSession session) throws Exception {
        PostgresServer server = ((PostgresServerConnection)session).getServer();
        Integer sessionId = statement.getSessionID();
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
                for (Integer sesId : server.getCurrentSessions()) {
                    server.cancelQuery(sesId);
                }
            } else if (server.getConnection(sessionId) != null) {
                server.cancelQuery(sessionId);
            } 
            sendComplete (session.getMessenger());
            break;
        case DISCONNECT_SESSION:
        case KILL_SESSION:
            if (sessionId == null) {
                for (Integer sesId : server.getCurrentSessions()) {
                    sendCompleteClosed(server.getConnection(sesId), "your session being disconnected");
                    server.killConnection(sesId);
                }
            }
            if (server.getConnection(sessionId) != null) {
                sendCompleteClosed(server.getConnection(sessionId), "your session being disconnected");
                server.killConnection(sessionId);
            }
            break;
        case SHUTDOWN:
            for (Integer sesId : server.getCurrentSessions()) {
                sendCompleteClosed(server.getConnection(sesId), "Akiban server being shutdown");
                server.killConnection(sesId);
            }
            shutdown(session, statement.isShutdownImmediate());
            break;
        }
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
    
    protected void sendCompleteClosed(PostgresServerConnection session, String reason) throws Exception {
        InvalidOperationException ex = new ConnectionTerminatedException (reason);
        session.sendErrorResponse(PostgresMessages.QUERY_TYPE, ex, ex.getCode(), ex.getMessage());
    }
    
    protected void shutdown (PostgresServerSession server, boolean immediate) throws Exception {
        MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        jmxServer.invoke(new ObjectName ("com.akiban:type=SHUTDOWN"), "shutdown", new Object[0], new String[0]);
    }
}
