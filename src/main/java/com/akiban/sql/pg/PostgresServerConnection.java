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

import com.akiban.server.types3.Types3Switch;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowPool;
import com.akiban.sql.server.ServerServiceRequirements;
import com.akiban.sql.server.ServerSessionBase;
import com.akiban.sql.server.ServerSessionTracer;
import com.akiban.sql.server.ServerStatementCache;
import com.akiban.sql.server.ServerTransaction;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.SQLParserException;
import com.akiban.sql.parser.StatementNode;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.*;
import com.akiban.server.service.EventTypes;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final PostgresServer server;
    private boolean running = false, ignoreUntilSync = false;
    private Socket socket;
    private PostgresMessenger messenger;
    private int sessionId, secret;
    private int version;
    private Map<String,PostgresStatement> preparedStatements =
        new HashMap<String,PostgresStatement>();
    private Map<String,PostgresBoundQueryContext> boundPortals =
        new HashMap<String,PostgresBoundQueryContext>();

    private ServerStatementCache<PostgresStatement> statementCache;
    private PostgresStatementParser[] unparsedGenerators;
    private PostgresStatementGenerator[] parsedGenerators;
    private Thread thread;
    
    private String sql;
    
    private volatile String cancelForKillReason, cancelByUser;

    public PostgresServerConnection(PostgresServer server, Socket socket, 
                                    int sessionId, int secret,
                                    ServerServiceRequirements reqs) {
        super(reqs);
        this.server = server;

        this.socket = socket;
        this.sessionId = sessionId;
        this.secret = secret;
        this.sessionTracer = new ServerSessionTracer(sessionId, server.isInstrumentationEnabled());
        sessionTracer.setRemoteAddress(socket.getInetAddress().getHostAddress());
    }

    public void start() {
        running = true;
        thread = new Thread(this);
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
        logger.info("Connect from {}" + socket.getRemoteSocketAddress());
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
                    sessionTracer.beginEvent(EventTypes.PROCESS);
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
                    sessionTracer.endEvent();
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

    protected void sendErrorResponse(PostgresMessages type, Exception exception, ErrorCode errorCode, String message)
        throws Exception
    {
        if (type.errorMode() == PostgresMessages.ErrorMode.NONE) throw exception;
        else {
            messenger.beginMessage(PostgresMessages.ERROR_RESPONSE_TYPE.code());
            messenger.write('S');
            messenger.writeString((type.errorMode() == PostgresMessages.ErrorMode.FATAL)
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
        if (type.errorMode() == PostgresMessages.ErrorMode.EXTENDED)
            ignoreUntilSync = true;
        else
            readyForQuery();
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

        if (Boolean.parseBoolean(properties.getProperty("require_password", "false"))) {
            messenger.beginMessage(PostgresMessages.AUTHENTICATION_TYPE.code());
            messenger.writeInt(PostgresMessenger.AUTHENTICATION_CLEAR_TEXT);
            messenger.sendMessage(true);
        }
        else {
            String user = properties.getProperty("user");
            logger.info("Login {}", user);
            authenticationOkay(user);
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
        logger.info("Login {}/{}", user, pass);
        authenticationOkay(user);
    }
    
    protected void authenticationOkay(String user) throws IOException {
        Properties status = new Properties();
        // This is enough to make the JDBC driver happy.
        status.put("client_encoding", properties.getProperty("client_encoding", "UNICODE"));
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

    protected void processQuery() throws IOException {
        long startTime = System.currentTimeMillis();
        int rowsProcessed = 0;
        sql = messenger.readString();
        sessionTracer.setCurrentStatement(sql);
        logger.info("Query: {}", sql);

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
                if (pstmt != null)
                    break;
            }
        }
        if (pstmt != null) {
            pstmt.sendDescription(context, false);
            rowsProcessed = executeStatement(pstmt, context, -1);
        }
        else {
            // Parse as a _list_ of statements and process each in turn.
            List<StatementNode> stmts;
            try {
                sessionTracer.beginEvent(EventTypes.PARSE);
                stmts = parser.parseStatements(sql);
            } 
            catch (SQLParserException ex) {
                throw new SQLParseException(ex);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            finally {
                sessionTracer.endEvent();
            }
            for (StatementNode stmt : stmts) {
                pstmt = generateStatement(stmt, null, null);
                if ((statementCache != null) && (stmts.size() == 1))
                    statementCache.put(sql, pstmt);
                pstmt.sendDescription(context, false);
                rowsProcessed = executeStatement(pstmt, context, -1);
            }
        }
        readyForQuery();
        logger.debug("Query complete");
        if (reqs.instrumentation().isQueryLogEnabled()) {
            reqs.instrumentation().logQuery(sessionId, sql, (System.currentTimeMillis() - startTime), rowsProcessed);
        }
    }

    protected void processParse() throws IOException {
        String stmtName = messenger.readString();
        sql = messenger.readString();
        short nparams = messenger.readShort();
        int[] paramTypes = new int[nparams];
        for (int i = 0; i < nparams; i++)
            paramTypes[i] = messenger.readInt();
        sessionTracer.setCurrentStatement(sql);
        logger.info("Parse: {}", sql);
        
        PostgresQueryContext context = new PostgresQueryContext(this);
        updateAIS(context);

        PostgresStatement pstmt = null;
        if (statementCache != null)
            pstmt = statementCache.get(sql);
        if (pstmt == null) {
            StatementNode stmt;
            List<ParameterNode> params;
            try {
                sessionTracer.beginEvent(EventTypes.PARSE);
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
                sessionTracer.endEvent();
            }
            pstmt = generateStatement(stmt, params, paramTypes);
            if (statementCache != null)
                statementCache.put(sql, pstmt);
        }
        preparedStatements.put(stmtName, pstmt);
        messenger.beginMessage(PostgresMessages.PARSE_COMPLETE_TYPE.code());
        messenger.sendMessage();
    }

    protected void processBind() throws IOException {
        String portalName = messenger.readString();
        String stmtName = messenger.readString();
        Object[] params = null;
        {
            boolean[] paramsBinary = null;
            short nformats = messenger.readShort();
            if (nformats > 0) {
                paramsBinary = new boolean[nformats];
                for (int i = 0; i < nformats; i++)
                    paramsBinary[i] = (messenger.readShort() == 1);
            }
            short nparams = messenger.readShort();
            if (nparams > 0) {
                params = new Object[nparams];
                boolean binary = false;
                for (int i = 0; i < nparams; i++) {
                    if (i < nformats)
                        binary = paramsBinary[i];
                    int len = messenger.readInt();
                    if (len < 0) continue;      // Null
                    byte[] param = new byte[len];
                    messenger.readFully(param, 0, len);
                    if (binary) {
                        params[i] = param;
                    }
                    else {
                        params[i] = new String(param, messenger.getEncoding());
                    }
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
        PostgresStatement pstmt = preparedStatements.get(stmtName);
        boundPortals.put(portalName, new PostgresBoundQueryContext(this, pstmt, 
                                                                   params, resultsBinary, defaultResultsBinary));
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
            pstmt = preparedStatements.get(name);
            context = new PostgresQueryContext(this);
            break;
        case (byte)'P':
            {
                PostgresBoundQueryContext bound = boundPortals.get(name);
                pstmt = bound.getStatement();
                context = bound;
            }
            break;
        default:
            throw new IOException("Unknown describe source: " + (char)source);
        }
        pstmt.sendDescription(context, true);
    }

    protected void processExecute() throws IOException {
        long startTime = System.nanoTime();
        int rowsProcessed = 0;
        String portalName = messenger.readString();
        int maxrows = messenger.readInt();
        PostgresBoundQueryContext context = boundPortals.get(portalName);
        PostgresStatement pstmt = context.getStatement();
        logger.info("Execute: {}", pstmt);
        rowsProcessed = executeStatement(pstmt, context, maxrows);
        logger.debug("Execute complete");
        if (reqs.instrumentation().isQueryLogEnabled()) {
            reqs.instrumentation().logQuery(sessionId, sql, (System.nanoTime() - startTime), rowsProcessed);
        }
    }

    protected void processClose() throws IOException {
        byte source = messenger.readByte();
        String name = messenger.readString();
        PostgresStatement pstmt;        
        switch (source) {
        case (byte)'S':
            pstmt = preparedStatements.remove(name);
            break;
        case (byte)'P':
            pstmt = boundPortals.remove(name).getStatement();
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
            // TODO: This could be more reliable if the AIS object itself
            // also knew its generation. Right now, can get new generation
            // # and old AIS and not notice until next change.
            long currentTimestamp = ddl.getTimestamp();
            if (aisTimestamp == currentTimestamp) 
                return;             // Unchanged.
            aisTimestamp = currentTimestamp;
            ais = ddl.getAIS(session);
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
            compiler = PostgresOperatorCompiler.create(this);
        else if (format.equals("json"))
            compiler = PostgresJsonCompiler.create(this); 
        else
            throw new InvalidParameterValueException(format);
        
        // Add the Persisitit Adapter - default for most tables
        adapters.put(StoreAdapter.AdapterType.PERSISTIT_ADAPTER, 
                new PersistitAdapter(compiler.getSchema(),
                                       reqs.store(),
                                       reqs.treeService(),
                                       session,
                                       reqs.config()));
        // Add the Memory Adapter - for the in memory tables
        adapters.put(StoreAdapter.AdapterType.MEMORY_ADAPTER, 
                new MemoryAdapter(compiler.getSchema(),
                                session,
                                reqs.config()));
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
            new PostgresServerStatementGenerator(this)
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
                                        aisTimestamp);
    }

    @Override
    public StoreAdapter getStore(final UserTable table) {
        if (table.hasMemoryTableFactory()) {
            return adapters.get(StoreAdapter.AdapterType.MEMORY_ADAPTER);
        }
        return adapters.get(StoreAdapter.AdapterType.PERSISTIT_ADAPTER);
    }

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

    protected PostgresStatement generateStatement(StatementNode stmt, 
                                                  List<ParameterNode> params,
                                                  int[] paramTypes) {
        try {
            sessionTracer.beginEvent(EventTypes.OPTIMIZE);
            for (PostgresStatementGenerator generator : parsedGenerators) {
                PostgresStatement pstmt = generator.generate(this, stmt, 
                                                             params, paramTypes);
                if (pstmt != null) return pstmt;
            }
        }
        finally {
            sessionTracer.endEvent();
        }
        throw new UnsupportedSQLException ("", stmt);
    }

    protected int executeStatement(PostgresStatement pstmt, PostgresQueryContext context, int maxrows)
            throws IOException {
        ServerTransaction localTransaction = beforeExecute(pstmt);
        int rowsProcessed = 0;
        boolean success = false;
        try {
            sessionTracer.beginEvent(EventTypes.EXECUTE);
            rowsProcessed = pstmt.execute(context, maxrows);
            success = true;
        }
        finally {
            afterExecute(pstmt, localTransaction, success);
            sessionTracer.endEvent();
        }
        return rowsProcessed;
    }

    @Override
    public LoadablePlan<?> loadablePlan(String planName)
    {
        return server.loadablePlan(planName);
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
        if (level.ordinal() <= maxNotificationLevel.ordinal()) {
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
    protected boolean propertySet(String key, String value) {
        if ("client_encoding".equals(key)) {
            messenger.setEncoding(value);
            return true;
        }
        boolean isNewtypes = false;
        if ("OutputFormat".equals(key) ||
            "parserInfixBit".equals(key) ||
            "parserInfixLogical".equals(key) ||
            "parserDoubleQuoted".equals(key) ||
            "columnAsFunc".equals(key) ||
            "cbo".equals(key) ||
            (isNewtypes = "newtypes".equals(key))) {
            if (parsedGenerators != null)
                rebuildCompiler();
            if (isNewtypes)
                Types3Switch.SET_ON = getBooleanProperty("newtypes", Types3Switch.DEFAULT);
            return true;
        }
        return super.propertySet(key, value);
    }

    /* MBean-related access */

    public boolean isInstrumentationEnabled() {
        return sessionTracer.isEnabled();
    }
    
    public void enableInstrumentation() {
        sessionTracer.enable();
    }
    
    public void disableInstrumentation() {
        sessionTracer.disable();
    }
    
    public String getSqlString() {
        return sql;
    }
    
    public String getRemoteAddress() {
        return socket.getInetAddress().getHostAddress();
    }
    
    public PostgresServer getServer() {
        return server;
    }

}
