package com.akiban.cserver;

import java.io.DataInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.admin.Admin;
import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.io.Reader;
import com.akiban.ais.io.Writer;
import com.akiban.ais.message.AISExecutionContext;
import com.akiban.ais.message.AISRequest;
import com.akiban.ais.message.AISResponse;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Source;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.AISPrinter;
import com.akiban.cserver.manage.MXBeanManager;
import com.akiban.cserver.message.ShutdownRequest;
import com.akiban.cserver.message.ShutdownResponse;
import com.akiban.cserver.message.ToStringWithRowDefCache;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.Store;
import com.akiban.message.AkibaConnection;
import com.akiban.message.AkibaConnectionImpl;
import com.akiban.message.ErrorResponse;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import com.akiban.message.MessageRegistry;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;
import com.akiban.util.Tap;

/**
 * @author peter
 */
public class CServer implements CServerConstants {

    private static final Log LOG = LogFactory.getLog(CServer.class.getName());

    private static final String AIS_DDL_NAME = "akiba_information_schema.ddl";

    private static final int GROUP_TABLE_ID_OFFSET = 1000000000;
    /**
     * Config property name and default for the port on which the CServer will
     * listen for requests.
     */
    // TODO: Why would it ever be anything other than localhost?
    // PDB: because the machine may have more than one NIC and localhost is
    // bound to only one of them.
    // private static final String P_CSERVER_HOST = "cserver.host";

    /**
     * Port on which the CServer will listen for requests.
     */
    public static final String CSERVER_PORT = System.getProperty(
            "cserver.port", DEFAULT_CSERVER_PORT_STRING);

    /**
     * Interface on which this cserver instance will listen. TODO - allow
     * multiple NICs
     */
    public static final String CSERVER_HOST = System.getProperty(
            "cserver.host", "localhost");

    /**
     * Config property name and default for setting of the verbose flag. When
     * true, many CServer methods log verbosely at INFO level.
     */

    private static final String VERBOSE_PROPERTY_NAME = "cserver.verbose";

    private static final String EXPERIMENTAL_PROPERTY_NAME = "cserver.experimental";

    /**
     * Name of this chunkserver. Must match one of the entries in
     * /config/cluster.properties (managed by Admin).
     */
    private static final String CSERVER_NAME = System
            .getProperty("cserver.name");

    private static Tap CSERVER_EXEC = Tap.add(new Tap.PerThread("cserver",
            Tap.TimeStampLog.class));

    private static int DEFAULT_MAX_CAPTURE_COUNT = 10;

    private final RowDefCache rowDefCache;
    private final CServerConfig config;
    private final PersistitStore store;
    private final String cserverPort;
    private AkibaInformationSchema ais0;
    private AkibaInformationSchema ais;
    private volatile boolean stopped;
    private Map<Integer, Thread> threadMap;
    private boolean leadCServer;
    private AISDistributor aisDistributor;

    private volatile int maxCaptureCount = DEFAULT_MAX_CAPTURE_COUNT;
    private volatile boolean enableMessageCapture;
    private List<CapturedMessage> capturedMessageList = new ArrayList<CapturedMessage>();
    private long lastSchemaGeneration = -1;

    /**
     * Construct a chunk server. If <tt>loadConfig</tt> is false then use
     * default unit test properties.
     * 
     * @param loadConfig
     * @throws Exception
     */
    public CServer(final boolean loadConfig) throws Exception {
        cserverPort = CSERVER_PORT;
        rowDefCache = new RowDefCache();
        if (loadConfig) {
            config = new CServerConfig();
            if (loadConfig) {
                config.load();
                if (config.getException() != null) {
                    LOG.fatal("CServer configuration failed");
                    throw new Exception("CServer configuration failed");
                }
            }
        } else {
            config = CServerConfig.unitTestConfig();
        }

        store = new PersistitStore(config, rowDefCache);
        threadMap = new TreeMap<Integer, Thread>();
    }

    public void start() throws Exception {
        boolean open = false;
        LOG.warn(String.format("Starting chunkserver %s on port %s",
                CSERVER_NAME, CSERVER_PORT));
        Tap.registerMXBean();
        MXBeanManager.registerMXBean(this, config);
        MessageRegistry.initialize();
        MessageRegistry.only().registerModule("com.akiban.cserver");
        MessageRegistry.only().registerModule("com.akiban.ais");
        MessageRegistry.only().registerModule("com.akiban.message");
        ais0 = primordialAIS();
        rowDefCache.setAIS(ais0);
        store.startUp();
        try {
            store.setVerbose(config.property(VERBOSE_PROPERTY_NAME, "false")
                    .equalsIgnoreCase("true"));
            store.setExperimental(config.property(EXPERIMENTAL_PROPERTY_NAME,
                    ""));
            store.setOrdinals();
            acquireAIS();
            if (false) {
                // TODO: Use this when we support multiple chunkservers
                Admin admin = Admin.only();
                leadCServer = admin.clusterConfig().leadChunkserver().name()
                        .equals(CSERVER_NAME);
                admin.markChunkserverUp(CSERVER_NAME);
                if (isLeader()) {
                    aisDistributor = new AISDistributor(this);
                }
            }
            NetworkHandlerFactory.initializeNetwork(CSERVER_HOST, CSERVER_PORT,
                    new ChannelNotifier());
            open = true;
        } finally {
            if (!open) {
                try {
                    store.shutDown();
                } catch (Exception e) {
                    // Not interesting -- we want to see the Exception
                    // that caused the startup failure.
                }
            }
        }
        LOG.warn(String.format("Started chunkserver %s on port %s, lead = %s",
                CSERVER_NAME, CSERVER_PORT, isLeader()));
    }

    public void stop() throws Exception {
        stopped = true;
        if (false) {
            // TODO: Use this when we support multiple chunkservers
            Admin.only().markChunkserverDown(CSERVER_NAME);
        }
        final List<Thread> copy;
        synchronized (threadMap) {
            copy = new ArrayList<Thread>(threadMap.values());
        }
        // for now I think this is the only way to make these threads
        // bail from their reads.
        for (final Thread thread : copy) {
            thread.interrupt();
        }
        try {
            NetworkHandlerFactory.closeNetwork();
        } finally {
            Tap.unregisterMXBean();
            store.shutDown();
        }
    }

    public String port() {
        return cserverPort;
    }

    public class ChannelNotifier implements CommEventNotifier {

        @Override
        public void onConnect(AkibaNetworkHandler handler) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Connection #" + handler.getId() + " created");
            }
            final String threadName = "CServer_" + handler.getId();
            final Thread thread = new Thread(new CServerRunnable(
                    AkibaConnectionImpl.createConnection(handler)), threadName);
            thread.setDaemon(true);
            thread.start();
            synchronized (threadMap) {
                threadMap.put(handler.getId(), thread);
            }
        }

        @Override
        public void onDisconnect(AkibaNetworkHandler handler) {
            final Thread thread;
            synchronized (threadMap) {
                thread = threadMap.remove(handler.getId());
            }
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
                if (LOG.isInfoEnabled()) {
                    LOG.info("Connection #" + handler.getId() + " ended");
                }
            } else {
                LOG.error("CServer thread for connection #" + handler.getId()
                        + " was missing or dead");
            }
        }
    }

    public PersistitStore getStore() {
        return store;
    }

    public class CServerContext implements ExecutionContext,
            AISExecutionContext, CServerShutdownExecutionContext {

        public Store getStore() {
            return store;
        }

        public AkibaInformationSchema ais() {
            return ais;
        }

        @Override
        public void executeRequest(AkibaConnection connection,
                AISRequest request) throws Exception {
            acquireAIS();
            AISResponse aisResponse = new AISResponse(ais);
            connection.send(aisResponse);
        }

        @Override
        public void executeResponse(AkibaConnection connection,
                AISResponse response) throws Exception {
            CServer.this.ais = response.ais();
            CServer.this.installAIS();
        }

        @Override
        public void executeRequest(AkibaConnection connection,
                ShutdownRequest request) throws Exception {
            if (LOG.isInfoEnabled()) {
                LOG.info("CServer stopping due to ShutdownRequest");
            }
            ShutdownResponse response = new ShutdownResponse();
            connection.send(response);
            stop();
        }

        public void installAIS(AkibaInformationSchema ais) throws Exception {
            LOG.info("Installing AIS");
            CServerAisTarget target = new CServerAisTarget(store);
            new Writer(target).save(ais);
            CServer.this.ais = ais;
            CServer.this.installAIS();
            LOG.info("AIS installation complete");
        }
    }

    /**
     * A Runnable that reads Network messages, acts on them and returns results.
     * 
     * @author peter
     * 
     */
    private class CServerRunnable implements Runnable {

        private final AkibaConnection connection;

        private final ExecutionContext context = new CServerContext();

        public CServerRunnable(final AkibaConnection connection) {
            this.connection = connection;
        }

        public void run() {

            Message message = null;
            long startTime = 0;
            long endTime = 0;
            long gapTime = 0;
            while (!stopped) {
                try {
                    message = connection.receive();
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Serving message " + message);
                    }

                    CSERVER_EXEC.in();

                    if (enableMessageCapture) {
                        startTime = System.nanoTime() / 1000;
                        gapTime = startTime - endTime;
                    }

                    CapturedMessage capturedMessage = null;

                    if (enableMessageCapture) {
                        synchronized (capturedMessageList) {
                            if (capturedMessageList.isEmpty()) {
                                gapTime = 0;
                            }
                            if (capturedMessageList.size() < maxCaptureCount) {
                                capturedMessage = new CapturedMessage(message);
                                capturedMessageList.add(capturedMessage);
                            }
                        }
                    }

                    if (store.isVerbose() && LOG.isInfoEnabled()) {
                        LOG
                                .info("Executing "
                                        + (message instanceof ToStringWithRowDefCache ? ((ToStringWithRowDefCache) message)
                                                .toString(rowDefCache)
                                                : message.toString()));
                    }

                    message.execute(connection, context);

                    if (capturedMessage != null) {
                        endTime = System.nanoTime() / 1000;
                        capturedMessage.finish(endTime - startTime, gapTime);
                    }

                    CSERVER_EXEC.out();

                } catch (InterruptedException e) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Thread " + Thread.currentThread().getName()
                                + (stopped ? " stopped" : " interrupted"));
                    }
                    break;
                } catch (Exception e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Unexpected error on " + message, e);
                    }
                    if (message != null) {
                        try {
                            connection.send(new ErrorResponse(e));
                        } catch (Exception f) {
                            LOG.error("Caught " + f.getClass()
                                    + " while sending error response to "
                                    + message + ": " + f.getMessage(), f);
                        }
                    }
                }
            }
        }
    }

    public static class CreateTableStruct {
        final int tableId;
        final String schemaName;
        final String tableName;
        final String ddl;

        public CreateTableStruct(int tableId, String schemaName,
                String tableName, String ddl) {
            this.tableId = tableId;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.ddl = ddl;
        }

        public String getDdl() {
            return ddl;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public int getTableId() {
            return tableId;
        }
    }

    public RowDefCache getRowDefCache() {
        return rowDefCache;
    }

    /**
     * Acquire an AkibaInformationSchema from MySQL and install it into the
     * local RowDefCache.
     * 
     * This method always refreshes the locally cached AkibaInformationSchema to
     * support schema modifications at the MySQL head.
     * 
     * @return an AkibaInformationSchema
     * @throws Exception
     */
    public synchronized void acquireAIS() throws Exception {
        LOG.warn(String.format("Acquiring AIS, experimental: %s", store
                .isExperimentalSchema()));
        if (store.isExperimentalSchema()) {
            final long generation = store.getSchemaGeneration();
            if (generation == lastSchemaGeneration) {
                return;
            }
            final List<CreateTableStruct> schema = store.getSchema();
            final StringBuilder sb = new StringBuilder();
            for (final CreateTableStruct tableStruct : schema) {
                sb.append("CREATE TABLE " + tableStruct.ddl
                        + CServerUtil.NEW_LINE);
            }
            final String schemaText = sb.toString();
            if (getStore().isVerbose() && LOG.isInfoEnabled()) {
                LOG.info("Acquiring AIS from schema: " + CServerUtil.NEW_LINE
                        + schemaText);
            }
            this.ais = new DDLSource().buildAISFromString(schemaText, ais0);

            for (final CreateTableStruct tableStruct : schema) {
                final UserTable table = ais.getUserTable(
                        tableStruct.schemaName, tableStruct.tableName);
                if (table != null) {
                    table.setTableId(tableStruct.tableId);
                    if (table.getParentJoin() == null) {
                        final GroupTable groupTable = table.getGroup()
                                .getGroupTable();
                        if (groupTable != null) {
                            groupTable.setTableId(tableStruct.tableId
                                    + GROUP_TABLE_ID_OFFSET);
                        }
                    }
                }
            }
            installAIS();
            new Writer(new CServerAisTarget(store)).save(ais);
            lastSchemaGeneration = generation;
        } else {
            final Source source = new CServerAisSource(store);
            this.ais = new Reader(source)
                    .load(new AkibaInformationSchema(ais0));
            installAIS();
        }
        LOG.warn("Acquired AIS");
    }

    boolean isLeader() {
        return leadCServer;
    }

    private synchronized void installAIS() throws Exception {
        if (LOG.isInfoEnabled()) {
            LOG.info("Installing " + ais.getDescription() + " in ChunkServer");
            LOG.debug(AISPrinter.toString(ais));
        }
        rowDefCache.clear();
        rowDefCache.setAIS(ais);
        store.setOrdinals();
        if (false) {
            // TODO: Use this when we support multiple chunkservers
            if (isLeader()) {
                assert aisDistributor != null;
                aisDistributor.distribute(ais);
            }
        }
    }

    /**
     * Loads the built-in primordial table definitions for the
     * akiba_information_schema tables.
     * 
     * @throws Exception
     */
    public AkibaInformationSchema primordialAIS() throws Exception {
        if (ais0 != null) {
            return ais0;
        }
        final DataInputStream stream = new DataInputStream(getClass()
                .getClassLoader().getResourceAsStream(AIS_DDL_NAME));
        // TODO: ugly, but gets the job done
        final StringBuilder sb = new StringBuilder();
        for (;;) {
            final String line = stream.readLine();
            if (line == null) {
                break;
            }
            sb.append(line);
            sb.append("\n");
        }
        ais0 = (new DDLSource()).buildAISFromString(sb.toString());
        return ais0;
    }

    public AkibaInformationSchema getAisCopy() {
        return new AkibaInformationSchema(ais);
    }

    /**
     * @param args
     *            the command line arguments
     */
    public static void main(String[] args) throws Exception {
        try {
            final CServer server = new CServer(true);
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        // HAZEL: MySQL Conference Demo 4/2010: MySQL/Drizzle/Memcache to Chunk
        // Server
        /*
         * com.thimbleware.jmemcached.protocol.MemcachedCommandHandler.registerCallback
         * ( new
         * com.thimbleware.jmemcached.protocol.MemcachedCommandHandler.Callback
         * () { public byte[] get(byte[] key) { byte[] result = null;
         * 
         * String request = new String(key); String[] tokens =
         * request.split(":"); if (tokens.length == 4) { String schema =
         * tokens[0]; String table = tokens[1]; String colkey = tokens[2];
         * String colval = tokens[3];
         * 
         * try { List<RowData> list = null; //list =
         * server.store.fetchRows(schema, table, colkey, colval, colval,
         * "order"); list = server.store.fetchRows(schema, table, colkey,
         * colval, colval, null);
         * 
         * StringBuilder builder = new StringBuilder(); for (RowData data: list)
         * { builder.append(data.toString(server.getRowDefCache()) + "\n");
         * //builder.append(data.toString()); }
         * 
         * result = builder.toString().getBytes(); } catch (Exception e) {
         * result = new String("read error: " + e.getMessage()).getBytes(); } }
         * else { result = new String("invalid key: " + request).getBytes(); }
         * 
         * return result; } }); com.thimbleware.jmemcached.Main.main(new
         * String[0]);
         */
    }

    public String property(final String key, final String dflt) {
        return config.property(key, dflt);
    }

    /**
     * For unit tests
     * 
     * @param key
     * @param value
     */
    public void setProperty(final String key, final String value) {
        config.setProperty(key, value);
    }

    public static class CapturedMessage {
        final long eventTime;
        long gap;
        long elapsed;
        final Message message;
        final String threadName;

        private CapturedMessage(final Message message) {
            this.eventTime = System.currentTimeMillis();
            this.message = message;
            this.threadName = Thread.currentThread().getName();
        }

        private void finish(final long elapsed, final long gap) {
            this.elapsed = elapsed;
            this.gap = gap;
        }

        public long getEventTime() {
            return eventTime;
        }

        public long getElapsedTime() {
            return elapsed;
        }

        public Message getMessage() {
            return message;
        }

        public String getThreadName() {
            return threadName;
        }

        private final static SimpleDateFormat SDF = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss.SSS");
        private final static String TO_STRING_FORMAT = "%12s  %s  elapsed=%,12dus  gap=%,12dus  %s";

        @Override
        public String toString() {
            return toString(null);
        }

        public String toString(final RowDefCache rowDefCache) {
            return String
                    .format(
                            TO_STRING_FORMAT,
                            threadName,
                            SDF.format(new Date(eventTime)),
                            elapsed,
                            gap,
                            message instanceof ToStringWithRowDefCache ? ((ToStringWithRowDefCache) message)
                                    .toString(rowDefCache)
                                    : message.toString());
        }
    }

    public void setMessageCaptureEnabled(final boolean enabled) {
        enableMessageCapture = enabled;
        if (enabled) {
            clearCapturedMessages();
        }
    }

    public void setMaxCapturedMessageCound(final int max) {
        maxCaptureCount = max;
    }

    public void clearCapturedMessages() {
        synchronized (capturedMessageList) {
            capturedMessageList.clear();
        }
    }

    public List<CapturedMessage> getCapturedMessageList() {
        synchronized (capturedMessageList) {
            return new ArrayList<CapturedMessage>(capturedMessageList);
        }
    }
}
