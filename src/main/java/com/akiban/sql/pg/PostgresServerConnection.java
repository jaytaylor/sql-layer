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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.sql.server.ServerServiceRequirements;
import com.akiban.sql.server.ServerSessionBase;
import com.akiban.sql.server.ServerSessionMonitor;
import com.akiban.sql.server.ServerStatement;
import com.akiban.sql.server.ServerStatementCache;
import com.akiban.sql.server.ServerTransaction;
import com.akiban.sql.server.ServerValueDecoder;
import com.akiban.sql.server.ServerValueEncoder;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.SQLParserException;
import com.akiban.sql.parser.StatementNode;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.*;
import com.akiban.server.service.monitor.MonitorStage;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ietf.jgss.*;
import java.security.Principal;
import java.security.PrivilegedAction;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import java.net.*;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.*;
import java.util.*;

/**
 * Connection to a Postgres server client.
 * Runs in its own thread; has its own AkServer Session.
 *
 */
public class PostgresServerConnection extends ServerSessionBase
                                      implements PostgresServerSession, Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresServerConnection.class);
    private static final InOutTap READ_MESSAGE = Tap.createTimer("PostgresServerConnection: read message");
    private static final InOutTap PROCESS_MESSAGE = Tap.createTimer("PostgresServerConnection: process message");
    private static final String THREAD_NAME_PREFIX = "PostgresServer_Session-"; // Session ID appended

    private final PostgresServer server;
    private boolean running = false, ignoreUntilSync = false;
    private Socket socket;
    private PostgresMessenger messenger;
    private ServerValueEncoder valueEncoder;
    private ServerValueDecoder valueDecoder;
    private OutputFormat outputFormat = OutputFormat.TABLE;
    private int sessionId, secret;
    private int version;
    private Map<String,PostgresPreparedStatement> preparedStatements =
        new HashMap<String,PostgresPreparedStatement>();
    private Map<String,PostgresBoundQueryContext> boundPortals =
        new HashMap<String,PostgresBoundQueryContext>();

    private ServerStatementCache<PostgresStatement> statementCache;
    private PostgresStatementParser[] unparsedGenerators;
    private PostgresStatementGenerator[] parsedGenerators;
    private Thread thread;

    private volatile String cancelForKillReason, cancelByUser;

    public PostgresServerConnection(PostgresServer server, Socket socket, 
                                    int sessionId, int secret,
                                    ServerServiceRequirements reqs) {
        super(reqs);
        this.server = server;

        this.socket = socket;
        this.sessionId = sessionId;
        this.secret = secret;
        this.sessionMonitor = new ServerSessionMonitor(PostgresServer.SERVER_TYPE, sessionId);
        sessionMonitor.setRemoteAddress(socket.getInetAddress().getHostAddress());
        reqs.monitor().registerSessionMonitor(sessionMonitor);
    }

    public void start() {
        running = true;
        thread = new Thread(this, THREAD_NAME_PREFIX + sessionId);
        thread.start();
    }

    public void stop() {
        running = false;
        // Can only wake up stream read by closing down socket.
        try {
            socket.close();
        }
        catch (IOException ex) {
        }
        if ((thread != null) && (thread != Thread.currentThread())) {
            try {
                // Wait a bit, but don't hang up shutdown if thread is wedged.
                thread.join(500);
                if (thread.isAlive())
                    logger.warn("Connection " + sessionId + " still running.");
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    public void run() {
        try {
            createMessenger();
            topLevel();
        }
        catch (Exception ex) {
            if (running)
                logger.warn("Error in server", ex);
        }
        finally {
            try {
                socket.close();
            }
            catch (IOException ex) {
            }
        }
    }

    protected void createMessenger() throws IOException {
        messenger = new PostgresMessenger(socket) {
                @Override
                public void beforeIdle() throws IOException {
                    super.beforeIdle();
                    sessionMonitor.enterStage(MonitorStage.IDLE);
                }

                @Override
                public void afterIdle() throws IOException {
                    sessionMonitor.leaveStage();
                    super.afterIdle();
                }

                @Override
                public void idle() {
                    if (cancelForKillReason != null) {
                        String msg = cancelForKillReason;
                        cancelForKillReason = null;
                        if (cancelByUser != null) {
                            msg += " by " + cancelByUser;
                            cancelByUser = null;
                        }
                        throw new ConnectionTerminatedException(msg);
                    }
                }
            };
    }

    protected void topLevel() throws IOException, Exception {
        logger.debug("Connect from {}" + socket.getRemoteSocketAddress());
        boolean startupComplete = false;
        try {
            while (running) {
                READ_MESSAGE.in();
                PostgresMessages type;
                try {
                    type = messenger.readMessage(startupComplete);
                } catch (ConnectionTerminatedException ex) {
                    logError(ErrorLogLevel.DEBUG, "About to terminate", ex);
                    notifyClient(QueryContext.NotificationLevel.WARNING,
                                 ex.getCode(), ex.getShortMessage());
                    stop();
                    continue;
                } finally {
                    READ_MESSAGE.out();
                }
                PROCESS_MESSAGE.in();
                if (ignoreUntilSync) {
                    if ((type != PostgresMessages.EOF_TYPE) && (type != PostgresMessages.SYNC_TYPE))
                        continue;
                    ignoreUntilSync = false;
                }
                long startNsec = System.nanoTime();
                try {
                    switch (type) {
                    case EOF_TYPE: // EOF
                        stop();
                        break;
                    case SYNC_TYPE:
                        readyForQuery();
                        break;
                    case STARTUP_MESSAGE_TYPE:
                        startupComplete = processStartupMessage();
                        break;
                    case PASSWORD_MESSAGE_TYPE:
                        processPasswordMessage();
                        break;
                    case QUERY_TYPE:
                        processQuery();
                        break;
                    case PARSE_TYPE:
                        processParse();
                        break;
                    case BIND_TYPE:
                        processBind();
                        break;
                    case DESCRIBE_TYPE:
                        processDescribe();
                        break;
                    case EXECUTE_TYPE:
                        processExecute();
                        break;
                    case FLUSH_TYPE:
                        processFlush();
                        break;
                    case CLOSE_TYPE:
                        processClose();
                        break;
                    case TERMINATE_TYPE:
                        processTerminate();
                        break;
                    }
                } catch (QueryCanceledException ex) {
                    InvalidOperationException nex = ex;
                    boolean forKill = false;
                    if (cancelForKillReason != null) {
                        nex = new ConnectionTerminatedException(cancelForKillReason);
                        nex.initCause(ex);
                        cancelForKillReason = null;
                        forKill = true;
                    }
                    logError(ErrorLogLevel.INFO, "Query canceled", nex);
                    String msg = nex.getShortMessage();
                    if (cancelByUser != null) {
                        if (!forKill) msg = "Query canceled";
                        msg += " by " + cancelByUser;
                        cancelByUser = null;
                    }
                    sendErrorResponse(type, nex, nex.getCode(), msg);
                    if (forKill) stop();
                } catch (ConnectionTerminatedException ex) {
                    logError(ErrorLogLevel.DEBUG, "Query terminated self", ex);
                    sendErrorResponse(type, ex, ex.getCode(), ex.getShortMessage());
                    stop();
                } catch (InvalidOperationException ex) {
                    logError(ErrorLogLevel.WARN, "Error in query", ex);
                    sendErrorResponse(type, ex, ex.getCode(), ex.getShortMessage());
                } catch (RollbackException ex) {
                    QueryRollbackException qe = new QueryRollbackException();
                    qe.initCause(ex);
                    logError(ErrorLogLevel.INFO, "Query rollback", qe);
                    sendErrorResponse(type, qe,  qe.getCode(), qe.getMessage());
                } catch (Exception ex) {
                    logError(ErrorLogLevel.WARN, "Unexpected error in query", ex);
                    String message = (ex.getMessage() == null ? ex.getClass().toString() : ex.getMessage());
                    sendErrorResponse(type, ex, ErrorCode.UNEXPECTED_EXCEPTION, message);
                }
                finally {
                    long stopNsec = System.nanoTime();
                    if (logger.isTraceEnabled()) {
                        logger.trace("Executed {}: {} usec", type, (stopNsec - startNsec) / 1000);
                    }
                }
                PROCESS_MESSAGE.out();
            }
        }
        finally {
            if (transaction != null) {
                transaction.abort();
                transaction = null;
            }
            server.removeConnection(sessionId);
            reqs.monitor().deregisterSessionMonitor(sessionMonitor);
        }
    }

    private enum ErrorLogLevel { WARN, INFO, DEBUG };

    private void logError(ErrorLogLevel level, String msg, Exception ex) {
        if (reqs.config().testing()) {
            level = ErrorLogLevel.DEBUG;
        }
        switch (level) {
        case DEBUG:
            logger.debug(msg, ex);
            break;
        case INFO:
            logger.info(msg, ex);
            break;
        case WARN:
        default:
            logger.warn(msg, ex);
            break;
        }
    }

    protected void sendErrorResponse(PostgresMessages type, Exception exception, ErrorCode errorCode, String message) throws Exception {
        PostgresMessages.ErrorMode errorMode = type.errorMode();
        if (errorMode == PostgresMessages.ErrorMode.NONE) {
            throw exception;
        }
        else {
            messenger.beginMessage(PostgresMessages.ERROR_RESPONSE_TYPE.code());
            messenger.write('S');
            messenger.writeString((errorMode == PostgresMessages.ErrorMode.FATAL)
                                  ? "FATAL" : "ERROR");
            messenger.write('C');
            messenger.writeString(errorCode.getFormattedValue());
            messenger.write('M');
            messenger.writeString(message);
            if (exception instanceof BaseSQLException) {
                int pos = ((BaseSQLException)exception).getErrorPosition();
                if (pos > 0) {
                    messenger.write('P');
                    messenger.writeString(Integer.toString(pos));
                }
            }
            messenger.write(0);
            messenger.sendMessage(true);
        }
        switch (errorMode) {
        case FATAL:
            stop();
            break;
        case EXTENDED:
            ignoreUntilSync = true;
            break;
        default:
            readyForQuery();
        }
    }

    protected void readyForQuery() throws IOException {
        messenger.beginMessage(PostgresMessages.READY_FOR_QUERY_TYPE.code());
        char mode = 'I';        // Idle
        if (isTransactionActive())
            mode = isTransactionRollbackPending() ? 'E' : 'T';
        messenger.writeByte(mode);
        messenger.sendMessage(true);
    }

    protected boolean processStartupMessage() throws IOException {
        int version = messenger.readInt();
        switch (version) {
        case PostgresMessenger.VERSION_CANCEL:
            processCancelRequest();
            return false;
        case PostgresMessenger.VERSION_SSL:
            processSSLMessage();
            return false;
        default:
            this.version = version;
            logger.debug("Version {}.{}", (version >> 16), (version & 0xFFFF));
        }

        Properties clientProperties = new Properties(server.getProperties());
        while (true) {
            String param = messenger.readString();
            if (param.length() == 0) break;
            String value = messenger.readString();
            clientProperties.put(param, value);
        }
        logger.debug("Properties: {}", clientProperties);
        setProperties(clientProperties);

        session = reqs.sessionService().createSession();
        // TODO: Not needed right now and not a convenient time to
        // encounter schema lock from long-running DDL.
        // But see comment in initParser(): what if we wanted to warn
        // or error when schema does not exist?
        //updateAIS(null);

        switch (server.getAuthenticationType()) {
        case NONE:
            {
                String user = properties.getProperty("user");
                logger.debug("Login {}", user);
                authenticationOkay(user);
            }
            break;
        case CLEAR_TEXT:
            {
                messenger.beginMessage(PostgresMessages.AUTHENTICATION_TYPE.code());
                messenger.writeInt(PostgresMessenger.AUTHENTICATION_CLEAR_TEXT);
                messenger.sendMessage(true);
            }
            break;
        case GSS:
            authenticationGSS();
            break;
        }

        return true;
    }

    protected void processCancelRequest() throws IOException {
        int sessionId = messenger.readInt();
        int secret = messenger.readInt();
        PostgresServerConnection connection = server.getConnection(sessionId);
        if ((connection != null) && (secret == connection.secret)) {
            connection.cancelQuery(null, null);
        }
        stop();                 // That's all for this connection.
    }

    protected void processSSLMessage() throws IOException {
        OutputStream raw = messenger.getOutputStream();
        if (System.getProperty("javax.net.ssl.keyStore") == null) {
            // JSSE doesn't have a keystore; TLSv1 handshake is gonna fail. Deny support.
            raw.write('N');
            raw.flush();
        }
        else {
            // Someone seems to have configured for SSL. Wrap the
            // socket and start server mode negotiation. Client should
            // then use SSL socket to start regular server protocol.
            raw.write('S');
            raw.flush();
            SSLSocketFactory sslFactory = (SSLSocketFactory)SSLSocketFactory.getDefault();
            SSLSocket sslSocket = (SSLSocket)sslFactory.createSocket(socket, socket.getLocalAddress().toString(), socket.getLocalPort(), true);
            socket = sslSocket;
            createMessenger();
            sslSocket.setUseClientMode(false);
            sslSocket.startHandshake();
        }
    }

    protected void processPasswordMessage() throws IOException {
        String user = properties.getProperty("user");
        String pass = messenger.readString();
        logger.debug("Login {}/{}", user, pass);
        authenticationOkay(user);
    }
    
    protected void authenticationOkay(String user) throws IOException {
        Properties status = new Properties();
        // This is enough to make the JDBC driver happy.
        status.put("client_encoding", properties.getProperty("client_encoding", "UTF8"));
        status.put("server_encoding", messenger.getEncoding());
        status.put("server_version", "8.4.7"); // Not sure what the min it'll accept is.
        status.put("session_authorization", user);
        status.put("DateStyle", "ISO, MDY");
        
        {
            messenger.beginMessage(PostgresMessages.AUTHENTICATION_TYPE.code());
            messenger.writeInt(PostgresMessenger.AUTHENTICATION_OK);
            messenger.sendMessage();
        }
        for (String prop : status.stringPropertyNames()) {
            messenger.beginMessage(PostgresMessages.PARAMETER_STATUS_TYPE.code());
            messenger.writeString(prop);
            messenger.writeString(status.getProperty(prop));
            messenger.sendMessage();
        }
        {
            messenger.beginMessage(PostgresMessages.BACKEND_KEY_DATA_TYPE.code());
            messenger.writeInt(sessionId);
            messenger.writeInt(secret);
            messenger.sendMessage();
        }
        readyForQuery();
    }

    protected void authenticationGSS() throws IOException {
        messenger.beginMessage(PostgresMessages.AUTHENTICATION_TYPE.code());
        messenger.writeInt(PostgresMessenger.AUTHENTICATION_GSS);
        messenger.sendMessage(true);
        
        final Subject gssLogin;
        try {
            gssLogin = server.getGSSLogin();
        }
        catch (LoginException ex) {
            throw new AuthenticationFailedException(ex); // or is this internal?
        }
        GSSName authenticated = Subject.doAs(gssLogin, new PrivilegedAction<GSSName>() {
                                                 @Override
                                                 public GSSName run() {
                                                     return gssNegotation(gssLogin);
                                                 }
                                             });
        logger.debug("Login {}", authenticated);
        authenticationOkay(authenticated.toString());
    }
    
    protected GSSName gssNegotation(Subject gssLogin) {
        String serverName = null;
        Iterator<Principal> iter = gssLogin.getPrincipals().iterator();
        if (iter.hasNext())
            serverName = iter.next().getName();
        try {
            GSSManager manager = GSSManager.getInstance();
            GSSCredential serverCreds = 
                manager.createCredential(manager.createName(serverName, null),
                                         GSSCredential.INDEFINITE_LIFETIME,
                                         new Oid("1.2.840.113554.1.2.2"), // krb5
                                         GSSCredential.ACCEPT_ONLY);
            GSSContext serverContext = manager.createContext(serverCreds);
            do {
                switch (messenger.readMessage(true)) {
                case PASSWORD_MESSAGE_TYPE:
                    break;
                default:
                    throw new AuthenticationFailedException("Protocol error: not password message");
                }
                byte[] token = messenger.getRawMessage(); // Note: not a String.
                token = serverContext.acceptSecContext(token, 0, token.length);
                if (token != null) {
                    messenger.beginMessage(PostgresMessages.AUTHENTICATION_TYPE.code());
                    messenger.writeInt(PostgresMessenger.AUTHENTICATION_GSS_CONTINUE);
                    messenger.write(token); // Again, no wrapping.
                    messenger.sendMessage(true);
                }
            } while (!serverContext.isEstablished());
            return serverContext.getSrcName();
        }
        catch (GSSException ex) {
            throw new AuthenticationFailedException(ex);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error reading message", ex);
        }
    }

    protected void processQuery() throws IOException {
        long startTime = System.currentTimeMillis();
        String sql = messenger.readString();
        logger.debug("Query: {}", sql);

        if (sql.length() == 0) {
            emptyQuery();
            return;
        }

        sessionMonitor.startStatement(sql, startTime);

        PostgresQueryContext context = new PostgresQueryContext(this);
        updateAIS(context);

        PostgresStatement pstmt = null;
        if (statementCache != null)
            pstmt = statementCache.get(sql);
        if (pstmt == null) {
            for (PostgresStatementParser parser : unparsedGenerators) {
                // Try special recognition first; only allowed to turn
                // into one statement.
                pstmt = parser.parse(this, sql, null);
                if (pstmt != null) {
                    pstmt.setAISGeneration(ais.getGeneration());
                    break;
                }
            }
        }
        int rowsProcessed = 0;
        if (pstmt != null) {
            pstmt.sendDescription(context, false);
            rowsProcessed = executeStatementWithAutoTxn(pstmt, context, -1);
        }
        else {
            // Parse as a _list_ of statements and process each in turn.
            List<StatementNode> stmts;
            try {
                sessionMonitor.enterStage(MonitorStage.PARSE);
                stmts = parser.parseStatements(sql);
            } 
            catch (SQLParserException ex) {
                throw new SQLParseException(ex);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            finally {
                sessionMonitor.leaveStage();
            }
            boolean singleStmt = (stmts.size() == 1);
            for (StatementNode stmt : stmts) {
                String stmtSQL;
                if (singleStmt)
                    stmtSQL = sql;
                else
                    stmtSQL = sql.substring(stmt.getBeginOffset(),
                                            stmt.getEndOffset() + 1);
                pstmt = generateStatementStub(stmtSQL, stmt, null, null);
                ServerTransaction local = beforeExecute(pstmt);
                boolean success = false;
                try {
                    pstmt = finishGenerating(context, pstmt, stmtSQL, stmt, null, null);
                    if ((statementCache != null) && singleStmt && pstmt.putInCache())
                        statementCache.put(stmtSQL, pstmt);
                    pstmt.sendDescription(context, false);
                    rowsProcessed = executeStatement(pstmt, context, -1);
                    success = true;
                } finally {
                    afterExecute(pstmt, local, success);
                }
            }
        }
        readyForQuery();
        sessionMonitor.endStatement(rowsProcessed);
        logger.debug("Query complete");
        if (reqs.monitor().isQueryLogEnabled()) {
            reqs.monitor().logQuery(sessionMonitor);
        }
    }

    protected void processParse() throws IOException {
        String stmtName = messenger.readString();
        String sql = messenger.readString();
        short nparams = messenger.readShort();
        int[] paramTypes = new int[nparams];
        for (int i = 0; i < nparams; i++)
            paramTypes[i] = messenger.readInt();
        sessionMonitor.startStatement(sql);
        logger.debug("Parse: {} = {}", stmtName, sql);
        
        PostgresQueryContext context = new PostgresQueryContext(this);
        updateAIS(context);

        PostgresStatement pstmt = null;
        if (statementCache != null)
            pstmt = statementCache.get(sql);
        if (pstmt == null) {
            StatementNode stmt;
            List<ParameterNode> params;
            try {
                sessionMonitor.enterStage(MonitorStage.PARSE);
                stmt = parser.parseStatement(sql);
                params = parser.getParameterList();
            } 
            catch (SQLParserException ex) {
                throw new SQLParseException(ex);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            finally {
                sessionMonitor.leaveStage();
            }
            pstmt = generateStatementStub(sql, stmt, params, paramTypes);
            ServerTransaction local = beforeExecute(pstmt);
            boolean success = false;
            try {
                pstmt = finishGenerating(context, pstmt, sql, stmt, params, paramTypes);
                success = true;
            } finally {
                afterExecute(pstmt, local, success);
            }
            if ((statementCache != null) && pstmt.putInCache()) {
                statementCache.put(sql, pstmt);
            }
        }
        preparedStatements.put(stmtName, new PostgresPreparedStatement(sql, pstmt));
        messenger.beginMessage(PostgresMessages.PARSE_COMPLETE_TYPE.code());
        messenger.sendMessage();
    }

    protected void processBind() throws IOException {
        String portalName = messenger.readString();
        String stmtName = messenger.readString();
        byte[][] params = null;
        boolean[] paramsBinary = null;
        {
            short nformats = messenger.readShort();
            if (nformats > 0) {
                paramsBinary = new boolean[nformats];
                for (int i = 0; i < nformats; i++)
                    paramsBinary[i] = (messenger.readShort() == 1);
            }
            short nparams = messenger.readShort();
            if (nparams > 0) {
                params = new byte[nparams][];
                for (int i = 0; i < nparams; i++) {
                    int len = messenger.readInt();
                    if (len < 0) continue;      // Null
                    byte[] param = new byte[len];
                    messenger.readFully(param, 0, len);
                    params[i] = param;
                }
            }
        }
        boolean[] resultsBinary = null; 
        boolean defaultResultsBinary = false;
        {        
            short nresults = messenger.readShort();
            if (nresults == 1)
                defaultResultsBinary = (messenger.readShort() == 1);
            else if (nresults > 0) {
                resultsBinary = new boolean[nresults];
                for (int i = 0; i < nresults; i++) {
                    resultsBinary[i] = (messenger.readShort() == 1);
                }
                defaultResultsBinary = resultsBinary[nresults-1];
            }
        }
        logger.debug("Bind: {} = {}", stmtName, portalName);
        PostgresPreparedStatement pstmt = preparedStatements.get(stmtName);
        PostgresStatement stmt = pstmt.getStatement();
        boolean canSuspend = ((stmt instanceof PostgresCursorGenerator) &&
                              ((PostgresCursorGenerator<?>)stmt).canSuspend(this));
        PostgresBoundQueryContext bound = 
            new PostgresBoundQueryContext(this, pstmt, canSuspend, true);
        if (params != null) {
            if (valueDecoder == null)
                valueDecoder = new ServerValueDecoder(messenger.getEncoding());
            PostgresType[] parameterTypes = null;
            boolean usePValues = false;
            if (stmt instanceof PostgresBaseStatement) {
                PostgresDMLStatement dml = (PostgresDMLStatement)stmt;
                parameterTypes = dml.getParameterTypes();
                usePValues = dml.usesPValues();
            }
            for (int i = 0; i < params.length; i++) {
                PostgresType pgType = null;
                if (parameterTypes != null)
                    pgType = parameterTypes[i];
                boolean binary = false;
                if ((paramsBinary != null) && (i < paramsBinary.length))
                    binary = paramsBinary[i];
                if (usePValues)
                    valueDecoder.decodePValue(params[i], pgType, binary, bound, i);
                else
                    valueDecoder.decodeValue(params[i], pgType, binary, bound, i);
            }
        }
        bound.setColumnBinary(resultsBinary, defaultResultsBinary);
        PostgresBoundQueryContext prev = boundPortals.put(portalName, bound);
        if (prev != null)
            prev.close();
        messenger.beginMessage(PostgresMessages.BIND_COMPLETE_TYPE.code());
        messenger.sendMessage();
    }

    protected void processDescribe() throws IOException{
        byte source = messenger.readByte();
        String name = messenger.readString();
        PostgresStatement pstmt;
        PostgresQueryContext context;
        switch (source) {
        case (byte)'S':
            pstmt = preparedStatements.get(name).getStatement();
            context = new PostgresQueryContext(this);
            break;
        case (byte)'P':
            {
                PostgresBoundQueryContext bound = boundPortals.get(name);
                pstmt = bound.getStatement().getStatement();
                context = bound;
            }
            break;
        default:
            throw new IOException("Unknown describe source: " + (char)source);
        }
        pstmt.sendDescription(context, true);
    }

    protected void processExecute() throws IOException {
        long startTime = System.currentTimeMillis();
        String portalName = messenger.readString();
        int maxrows = messenger.readInt();
        logger.debug("Execute: {}", portalName);
        PostgresBoundQueryContext context = boundPortals.get(portalName);
        PostgresPreparedStatement pstmt = context.getStatement();
        sessionMonitor.startStatement(pstmt.getSQL(), startTime);
        int rowsProcessed = executeStatementWithAutoTxn(pstmt.getStatement(), context, maxrows);
        sessionMonitor.endStatement(rowsProcessed);
        logger.debug("Execute complete: {} rows", rowsProcessed);
        if (reqs.monitor().isQueryLogEnabled()) {
            reqs.monitor().logQuery(sessionMonitor);
        }
    }

    protected void processFlush() throws IOException {
        messenger.flush();
    }

    protected void processClose() throws IOException {
        byte source = messenger.readByte();
        String name = messenger.readString();
        switch (source) {
        case (byte)'S':
            deallocatePreparedStatement(name);
            break;
        case (byte)'P':
            closeBoundPortal(name);
            break;
        default:
            throw new IOException("Unknown describe source: " + (char)source);
        }
        messenger.beginMessage(PostgresMessages.CLOSE_COMPLETE_TYPE.code());
        messenger.sendMessage();
    }
    
    protected void processTerminate() throws IOException {
        stop();
    }

    public void cancelQuery(String forKillReason, String byUser) {
        this.cancelForKillReason = forKillReason;
        this.cancelByUser = byUser;
        // A running query checks session state for query cancelation during Cursor.next() calls. If the
        // query is stuck in a blocking operation, then thread interruption should unstick it. Either way,
        // the query should eventually throw QueryCanceledException which will be caught by topLevel().
        if (session != null) {
            session.cancelCurrentQuery(true);
        }
        if (thread != null) {
            thread.interrupt();
        }
    }

    public void waitAndStop() {
        // Wait a little bit for the connection to stop itself.
        for (int i = 0; i < 5; i++) {
            if (!running) return;
            try {
                Thread.sleep(50);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // Force stop.
        stop();
    }

    // When the AIS changes, throw everything away, since it might
    // point to obsolete objects.
    protected void updateAIS(PostgresQueryContext context) {
        boolean locked = false;
        try {
            if (context != null) {
                // If there is long-running DDL like creating an index, this is almost
                // always where other queries will lock.
                context.lock(DXLFunction.UNSPECIFIED_DDL_READ);
                locked = true;
            }
            DDLFunctions ddl = reqs.dxl().ddlFunctions();
            AkibanInformationSchema newAIS = ddl.getAIS(session);
            if ((ais != null) && (ais.getGeneration() == newAIS.getGeneration()))
                return;             // Unchanged.
            ais = newAIS;
        }
        finally {
            if (locked) {
                context.unlock(DXLFunction.UNSPECIFIED_DDL_READ);
            }
        }
        rebuildCompiler();
    }

    protected void rebuildCompiler() {
        Object parserKeys = initParser();

        PostgresOperatorCompiler compiler;
        String format = getProperty("OutputFormat", "table");
        if (format.equals("table"))
            outputFormat = OutputFormat.TABLE;
        else if (format.equals("json"))
            outputFormat = OutputFormat.JSON;
        else if (format.equals("json_with_meta_data"))
            outputFormat = OutputFormat.JSON_WITH_META_DATA;
        else
            throw new InvalidParameterValueException(format);
        switch (outputFormat) {
        case TABLE:
        default:
            compiler = PostgresOperatorCompiler.create(this);
            break;
        case JSON:
        case JSON_WITH_META_DATA:
            compiler = PostgresJsonCompiler.create(this);
            break;
        }

        initAdapters(compiler);

        unparsedGenerators = new PostgresStatementParser[] {
            new PostgresEmulatedMetaDataStatementParser(this)
        };
        parsedGenerators = new PostgresStatementGenerator[] {
            // Can be ordered by frequency so long as there is no overlap.
            compiler,
            new PostgresDDLStatementGenerator(this),
            new PostgresSessionStatementGenerator(this),
            new PostgresCallStatementGenerator(this),
            new PostgresExplainStatementGenerator(this),
            new PostgresServerStatementGenerator(this),
            new PostgresCursorStatementGenerator(this)
        };

        statementCache = getStatementCache();
    }

    protected ServerStatementCache<PostgresStatement>  getStatementCache() {
        // Statement cache depends on some connection settings.
        return server.getStatementCache(Arrays.asList(parser.getFeatures(),
                                                      defaultSchemaName,
                                                      getProperty("OutputFormat", "table"),
                                                      getBooleanProperty("cbo", true),
                                                      getBooleanProperty("newtypes", false)),
                                        ais.getGeneration());
    }

    @Override
    protected void sessionChanged() {
        if (parsedGenerators == null) return; // setAttribute() from generator's ctor.
        for (PostgresStatementParser parser : unparsedGenerators) {
            parser.sessionChanged(this);
        }
        for (PostgresStatementGenerator generator : parsedGenerators) {
            generator.sessionChanged(this);
        }
        statementCache = getStatementCache();
    }

    protected PostgresStatement generateStatementStub(String sql, StatementNode stmt,
                                                      List<ParameterNode> params,
                                                      int[] paramTypes) {
        try {
            sessionMonitor.enterStage(MonitorStage.OPTIMIZE);
            for (PostgresStatementGenerator generator : parsedGenerators) {
                PostgresStatement pstmt = generator.generateStub(this, sql, stmt,
                                                                 params, paramTypes);
                if (pstmt != null)
                    return pstmt;
            }
        }
        finally {
            sessionMonitor.leaveStage();
        }
        throw new UnsupportedSQLException ("", stmt);
    }

    protected PostgresStatement finishGenerating(PostgresQueryContext context, PostgresStatement pstmt,
                                                 String sql, StatementNode stmt,
                                                 List<ParameterNode> params,
                                                 int[] paramTypes) {
        try {
            sessionMonitor.enterStage(MonitorStage.OPTIMIZE);
            updateAIS(context);
            PostgresStatement newpstmt = pstmt.finishGenerating(this, sql, stmt, params, paramTypes);
            if (!newpstmt.hasAISGeneration())
                newpstmt.setAISGeneration(ais.getGeneration());
            return newpstmt;
        }
        finally {
            sessionMonitor.leaveStage();
        }
    }

    protected int executeStatementWithAutoTxn(PostgresStatement pstmt, PostgresQueryContext context, int maxrows)
            throws IOException {
        ServerTransaction localTransaction = beforeExecute(pstmt);
        int rowsProcessed;
        boolean success = false;
        try {
            rowsProcessed = executeStatement(pstmt, context, maxrows);
            success = true;
        }
        finally {
            afterExecute(pstmt, localTransaction, success);
            sessionMonitor.leaveStage();
        }
        return rowsProcessed;
    }

    protected int executeStatement(PostgresStatement pstmt, PostgresQueryContext context, int maxrows)
            throws IOException {
        int rowsProcessed;
        PersistitAdapter persistitAdapter = null;
        if ((transaction != null) &&
            // As opposed to WRITE_STEP_ISOLATED.
            (pstmt.getTransactionMode() == PostgresStatement.TransactionMode.WRITE)) {
            persistitAdapter = (PersistitAdapter)adapters.get(StoreAdapter.AdapterType.PERSISTIT_ADAPTER);
            persistitAdapter.withStepChanging(false);
        }
        try {
            if (pstmt.getAISGenerationMode() == ServerStatement.AISGenerationMode.NOT_ALLOWED) {
                updateAIS(context);
                if (pstmt.getAISGeneration() != ais.getGeneration())
                    throw new StaleStatementException();
            }
            session.setTimeoutAfterSeconds(getQueryTimeoutSec());
            sessionMonitor.enterStage(MonitorStage.EXECUTE);
            rowsProcessed = pstmt.execute(context, maxrows);
        }
        finally {
            if (persistitAdapter != null)
                persistitAdapter.withStepChanging(true); // Keep conservative default.
            sessionMonitor.leaveStage();
        }
        return rowsProcessed;
    }

    protected void emptyQuery() throws IOException {
        messenger.beginMessage(PostgresMessages.EMPTY_QUERY_RESPONSE_TYPE.code());
        messenger.sendMessage();
        readyForQuery();
    }

    @Override
    public void prepareStatement(String name, 
                                 String sql, StatementNode stmt,
                                 List<ParameterNode> params, int[] paramTypes) {
        PostgresQueryContext context = new PostgresQueryContext(this);
        PostgresStatement pstmt = generateStatementStub(sql, stmt, params, paramTypes);
        ServerTransaction local = beforeExecute(pstmt);
        boolean success = false;
        try {
            pstmt = finishGenerating(context, pstmt, sql, stmt, params, paramTypes);
            success = true;
        } 
        finally {
            afterExecute(pstmt, local, success);
        }
        preparedStatements.put(name, new PostgresPreparedStatement(sql, pstmt));
    }

    @Override
    public int executePreparedStatement(PostgresExecuteStatement estmt, int maxrows)
            throws IOException {
        PostgresPreparedStatement pstmt = preparedStatements.get(estmt.getName());
        PostgresBoundQueryContext context = 
            new PostgresBoundQueryContext(this, pstmt, false, false);
        estmt.setParameters(context);
        sessionMonitor.startStatement(pstmt.getSQL(), System.currentTimeMillis());
        pstmt.getStatement().sendDescription(context, false);
        int nrows = executeStatementWithAutoTxn(pstmt.getStatement(), context, maxrows);
        sessionMonitor.endStatement(nrows);
        return nrows;
    }

    @Override
    public void deallocatePreparedStatement(String name) {
        PostgresPreparedStatement pstmt = preparedStatements.remove(name);
    }

    @Override
    public void declareStatement(String name, 
                                 String sql, StatementNode stmt) {
        PostgresQueryContext context = new PostgresQueryContext(this);
        PostgresStatement pstmt = generateStatementStub(sql, stmt, null, null);
        ServerTransaction local = beforeExecute(pstmt);
        boolean success = false;
        try {
            pstmt = finishGenerating(context, pstmt, sql, stmt, null, null);
            success = true;
        } 
        finally {
            afterExecute(pstmt, local, success);
        }
        PostgresPreparedStatement ppstmt;
        PostgresExecuteStatement estmt = null;
        if (pstmt instanceof PostgresExecuteStatement) {
            // DECLARE ... EXECUTE ... gets spliced out rather than
            // making a second prepared statement.
            estmt = (PostgresExecuteStatement)pstmt;
            ppstmt = preparedStatements.get(estmt.getName());
            pstmt = ppstmt.getStatement();
        }
        else {
            ppstmt = new PostgresPreparedStatement(sql, pstmt);
        }
        if (!(pstmt instanceof PostgresCursorGenerator)) {
            throw new UnsupportedSQLException("DECLARE can only be used with a result-generating statement", stmt);
        }
        if (!((PostgresCursorGenerator<?>)pstmt).canSuspend(this)) {
            throw new UnsupportedSQLException("DECLARE can only be used within a transaction", stmt);
        }
        PostgresBoundQueryContext bound =
            new PostgresBoundQueryContext(this, ppstmt, true, false);
        if (estmt != null) {
            estmt.setParameters(bound);
        }
        PostgresBoundQueryContext prev = boundPortals.put(name, bound);
        if (prev != null)
            prev.close();
    }

    @Override
    public int fetchStatement(String name, int count) throws IOException {
        PostgresBoundQueryContext bound = boundPortals.get(name);
        PostgresPreparedStatement pstmt = bound.getStatement();
        sessionMonitor.startStatement(pstmt.getSQL(), System.currentTimeMillis());
        pstmt.getStatement().sendDescription(bound, false);
        int nrows = executeStatementWithAutoTxn(pstmt.getStatement(), bound, count);
        sessionMonitor.endStatement(nrows);
        return nrows;
    }

    @Override
    public void closeBoundPortal(String name) {
        PostgresBoundQueryContext bound = boundPortals.remove(name);
        bound.close();
    }

    @Override
    public Date currentTime() {
        Date override = server.getOverrideCurrentTime();
        if (override != null)
            return override;
        else
            return super.currentTime();
    }

    @Override
    public void notifyClient(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) 
            throws IOException {
        if (shouldNotify(level)) {
            Object state = messenger.suspendMessage();
            messenger.beginMessage(PostgresMessages.NOTICE_RESPONSE_TYPE.code());
            messenger.write('S');
            switch (level) {
            case WARNING:
                messenger.writeString("WARN");
                break;
            case INFO:
                messenger.writeString("INFO");
                break;
            case DEBUG:
                messenger.writeString("DEBUG");
                break;
            // Other possibilities are "NOTICE" and "LOG".
            }
            if (errorCode != null) {
                messenger.write('C');
                messenger.writeString(errorCode.getFormattedValue());
            }
            messenger.write('M');
            messenger.writeString(message);
            messenger.write(0);
            messenger.sendMessage(true);
            messenger.resumeMessage(state);
        }
    }

    /* PostgresServerSession */

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public PostgresMessenger getMessenger() {
        return messenger;
    }

    @Override
    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    @Override
    public ServerValueEncoder getValueEncoder() {
        if (valueEncoder == null)
            valueEncoder = new ServerValueEncoder(messenger.getEncoding(), 
                                                  getZeroDateTimeBehavior());
        return valueEncoder;
    }

    @Override
    protected boolean propertySet(String key, String value) {
        if ("client_encoding".equals(key)) {
            messenger.setEncoding(value);
            valueEncoder = null; // These depend on the encoding.
            valueDecoder = null;
            return true;
        }
        if ("OutputFormat".equals(key) ||
            "parserInfixBit".equals(key) ||
            "parserInfixLogical".equals(key) ||
            "parserDoubleQuoted".equals(key) ||
            "columnAsFunc".equals(key) ||
            "cbo".equals(key) ||
            "newtypes".equals(key)) {
            if (parsedGenerators != null)
                rebuildCompiler();
            return true;
        }
        if ("zeroDateTimeBehavior".equals(key)) {
            valueEncoder = null; // Also depends on this.
        }
        return super.propertySet(key, value);
    }
    
    public PostgresServer getServer() {
        return server;
    }

}
