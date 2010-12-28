package com.akiban.cserver.service.tree;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.StorageLink;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import com.persistit.logging.ApacheCommonsLogAdapter;

public class TreeServiceImpl implements TreeService,
        Service<TreeService> {

    private final static int MEGA = 1024 * 1024;

    private static final Log LOG = LogFactory.getLog(TreeServiceImpl.class
            .getName());

    private static final String SERVER_MODULE_NAME = "cserver";

    private static final String PERSISTIT_MODULE_NAME = "persistit";

    private static final String DATAPATH_PROP_NAME = "datapath";

    private static final String BUFFER_SIZE_PROP_NAME = "buffersize";

    private static final String BUFFER_COUNT_PROP_NAME = "buffercount";

    private static final String DEFAULT_DATAPATH = "/tmp/chunkserver_data";

    // Must be one of 1024, 2048, 4096, 8192, 16384:
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    // Generally this is used only for unit tests and is
    // overridden by memory allocation calculation.
    private static final int DEFAULT_BUFFER_COUNT = 1024;

    private final static long MEMORY_RESERVATION = 64 * MEGA;

    private final static float PERSISTIT_ALLOCATION_FRACTION = 0.5f;

    private static final String FIXED_ALLOCATION_PROPERTY_NAME = "fixed";

    static final int MAX_TRANSACTION_RETRY_COUNT = 10;

    private final ConfigurationService configService;

    private final static AtomicInteger INSTANCE_COUNT = new AtomicInteger();

    private Persistit db;

    public TreeServiceImpl(final ConfigurationService configService) {
        this.configService = configService;
    }

    public synchronized void start() throws Exception {
        assert db == null;
        // TODO - remove this when sure we don't need it
        assert INSTANCE_COUNT.incrementAndGet() == 1;
        final Properties properties = configService.getModuleConfiguration(
                PERSISTIT_MODULE_NAME).getProperties();
        //
        // This section modifies the properties gotten from the
        // default configuration plus chunkserver.properties. It
        //
        // (a) copies cserver.datapath to datapath
        // (b) sets the buffersize property if null
        // (c) sets the buffercount property if null.
        //
        // Copies the cserver.datapath property to the Persistit properties set.
        // This allows Persistit to perform substitution of ${datapath} with
        // the server-specified home directory.
        //
        final String datapath = configService.getProperty(SERVER_MODULE_NAME,
                DATAPATH_PROP_NAME, DEFAULT_DATAPATH);
        properties.setProperty(DATAPATH_PROP_NAME, datapath);
        ensureDirectoryExists(datapath, false);

        final boolean isFixedAllocation = "true".equals(configService
                .getProperty(SERVER_MODULE_NAME,
                        FIXED_ALLOCATION_PROPERTY_NAME, "false"));
        if (!properties.contains(BUFFER_SIZE_PROP_NAME)) {
            properties.setProperty(BUFFER_SIZE_PROP_NAME,
                    String.valueOf(DEFAULT_BUFFER_SIZE));
        }
        final int bufferSize = Integer.parseInt(properties
                .getProperty(BUFFER_SIZE_PROP_NAME));
        if (!properties.contains(BUFFER_COUNT_PROP_NAME)) {
            properties.setProperty(BUFFER_COUNT_PROP_NAME,
                    String.valueOf(bufferCount(bufferSize, isFixedAllocation)));
        }
        //
        // Now we're ready to create the Persistit instance.
        //
        db = new Persistit();
        db.setPersistitLogger(new ApacheCommonsLogAdapter(LOG));
        db.initialize(properties);

        if (LOG.isInfoEnabled()) {
            LOG.info("PersistitStore datapath=" + db.getProperty("datapath")
                    + (bufferSize / 1024) + "k_buffers="
                    + db.getProperty("buffer.count." + bufferSize));
        }

    }

    /**
     * Makes sure the given directory exists, optionally trying to create it.
     * 
     * @param path
     *            the directory to check for or create
     * @param alreadyTriedCreatingDirectory
     *            whether we've already tried to create the directory
     * @throws FileNotFoundException
     *             if the given path exists but is not a directory, or can't be
     *             created
     */
    private static void ensureDirectoryExists(String path,
            boolean alreadyTriedCreatingDirectory) throws FileNotFoundException {
        File dir = new File(path);
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new FileNotFoundException(String.format(
                        "%s exists but is not a directory", dir));
            }
        } else {
            if (alreadyTriedCreatingDirectory) {
                throw new FileNotFoundException(String.format(
                        "Unable to create directory %s. Permissions problem?",
                        dir));
            } else {
                dir.mkdirs();
                ensureDirectoryExists(path, true);
            }
        }
    }

    private int bufferCount(final int bufferSize,
            final boolean isFixedAllocation) {
        if (isFixedAllocation) {
            return DEFAULT_BUFFER_COUNT;
        }
        final long allocation = (long) ((CServerUtil.availableMemory() - MEMORY_RESERVATION) * PERSISTIT_ALLOCATION_FRACTION);
        final int allocationPerBuffer = (int) (bufferSize * 1.5);
        return Math.max(512, (int) (allocation / allocationPerBuffer));
    }

    public synchronized void stop() throws Exception {
        // TODO - remove this when sure we don't need it
        assert INSTANCE_COUNT.decrementAndGet() == 0;
        if (db != null) {
            db.shutdownGUI();
            db.close();
            db = null;
        }
    }

    @Override
    public TreeService cast() {
        return this;
    }

    @Override
    public Class<TreeService> castClass() {
        return TreeService.class;
    }

    @Override
    public Persistit getDb() {
        return db;
    }

    @Override
    public Exchange getExchange(final Session session, final StorageLink link)
            throws PersistitException {
        Tree tree = (Tree) link.getStorageCache();
        if (tree == null) {
            Volume volume = mappedVolume(link);
            tree = volume.getTree(link.getTreeName(), true);
            link.setStorageCache(tree);
        }
        return getExchange(session, tree);
    }

    @Override
    public Exchange getExchange(final Session session, final Tree tree)
            throws PersistitException {
        final List<Exchange> list = exchangeList(session, tree);
        if (list.isEmpty()) {
            return new Exchange(tree);
        } else {
            return list.remove(list.size() - 1);
        }
    }

    @Override
    public void releaseExchange(final Session session, final Exchange exchange) {
        exchange.getKey().clear();
        exchange.getValue().clear();
        final List<Exchange> list = exchangeList(session, exchange.getTree());
        list.add(exchange);
    }

    @Override
    public Transaction getTransaction(final Session session) {
        return db.getTransaction();
    }

    @Override
    public long getTimestamp(final Session session) {
        return db.getTransaction().getTimestamp();
    }

    @Override
    public void visitStorage(final Session session,
            final TreeVisitor visitor, final String treeName)
            throws Exception {
        final Volume sysVol = db.getSystemVolume();
        final Volume txnVol = db.getTransactionVolume();
        for (final Volume volume : db.getVolumes()) {
            if (volume != sysVol && volume != txnVol) {
                final Tree tree = volume.getTree(treeName, false);
                if (tree != null) {
                    final Exchange exchange = getExchange(session, tree);
                    visitor.visit(exchange);
                }
            }
        }
    }

    @Override
    public int volumeHandle(final Exchange exchange) {
        // TODO - implement this
        return 0;
    }

    @Override
    public boolean isContainer(final Exchange exchange,
            final StorageLink storageLink) {
        final Volume volume = mappedVolume(storageLink);
        return exchange.getVolume().equals(volume);
    }

    private Volume mappedVolume(final StorageLink link) {
        // TODO - Tablespace Mapping
        return db.getVolume(VOLUME_NAME);
    }

    private List<Exchange> exchangeList(final Session session, final Tree tree) {
        Map<Tree, List<Exchange>> map = session.get("persistit", "exchangemap");
        List<Exchange> list;
        if (map == null) {
            map = new HashMap<Tree, List<Exchange>>();
            session.put("persistit", "exchangemap", map);
            list = new ArrayList<Exchange>();
            map.put(tree, list);
        } else {
            list = map.get(tree);
            if (list == null) {
                list = new ArrayList<Exchange>();
                map.put(tree, list);
            }
        }
        return list;
    }
}
