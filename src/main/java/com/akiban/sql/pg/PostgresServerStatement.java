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

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.error.UnsupportedConfigurationException;
import com.akiban.sql.parser.AlterServerNode;

public class PostgresServerStatement implements PostgresStatement {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresServerStatement.class);
    private final AlterServerNode statement;

    public PostgresServerStatement (AlterServerNode stmt) {
        this.statement = stmt;
    }
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
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
        PostgresServerSession server = context.getServer();
        doOperation(server);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(statement.statementToString());
            messenger.sendMessage();
        }
        return 0;
    }
    
    protected void doOperation (PostgresServerSession session) {
        PostgresServer server = ((PostgresServerConnection)session).getServer();
        Integer sessionId = statement.getSessionID();
        switch (statement.getAlterSessionType()) {
        case SET_SERVER_VARIABLE:
            setVariable (session, statement.getVariable(), statement.getValue());
            break;
        case INTERRUPT_SESSION:
            if (sessionId == null) {
                for (Integer sesId : server.getCurrentSessions()) {
                    server.cancelQuery(sesId);
                }
            } else if (server.getConnection(sessionId) != null) {
                server.cancelQuery(sessionId);
            } 
            break;
        case DISCONNECT_SESSION:
        case KILL_SESSION:
            if (sessionId == null) {
                for (Integer sesId : server.getCurrentSessions()) {
                    server.killConnection(sesId);
                }
            }
            if (server.getConnection(sessionId.intValue()) != null) {
                server.killConnection(sessionId);
            }
            break;
        case SHUTDOWN:
            shutdown(session, statement.isShutdownImmediate());
            break;
        }
    }

    protected void setVariable(PostgresServerSession server, String variable, String value) {
        if (!Arrays.asList(PostgresSessionStatement.ALLOWED_CONFIGURATION).contains(variable))
            throw new UnsupportedConfigurationException (variable);
        server.setProperty(variable, value);
    }
    
    protected void shutdown (PostgresServerSession server, boolean immediate) {
        MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName mbeanName = null;

        PostgresMessenger messenger = server.getMessenger();
        try {
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(statement.statementToString());
            messenger.sendMessage();
        } catch (IOException e1) {
            
        }
        
        try {
            mbeanName = new ObjectName ("com.akiban:type=SHUTDOWN");
        } catch (MalformedObjectNameException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NullPointerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            jmxServer.invoke(mbeanName, "shutdown", new Object[0], new String[0]);
        } catch (InstanceNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ReflectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MBeanException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
