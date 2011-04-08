/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.tree;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.AkServerUtil;
import com.akiban.server.TableStatusCache;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.Volume;
import com.persistit.VolumeSpecification;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.PersistitException;

public class TreeServiceImpl implements TreeService, Service<TreeService>,
        JmxManageable {

    private final static Session.Key<Map<Tree, List<Exchange>>> EXCHANGE_MAP = Session.Key.of("exchangemap");

    private final static int MEGA = 1024 * 1024;

    private static final Logger LOG = LoggerFactory
            .getLogger(TreeServiceImpl.class.getName());

    private static final String PERSISTIT_MODULE_NAME = "persistit";

    private static final String DATAPATH_PROP_NAME = "datapath";

    private static final String BUFFER_SIZE_PROP_NAME = "buffersize";

    private static final String BUFFER_COUNT_PROP_NAME = "buffer.count.";

    private static final String DEFAULT_DATAPATH = "/tmp/akiban_server";

    // Must be one of 1024, 2048, 4096, 8192, 16384:
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    // Generally this is used only for unit tests and is
    // overridden by memory allocation calculation.
    private static final int DEFAULT_BUFFER_COUNT = 1024;

    private final static long MEMORY_RESERVATION = 64 * MEGA;

    private final static float PERSISTIT_ALLOCATION_FRACTION = 0.5f;

    private static final String FIXED_ALLOCATION_PROPERTY_NAME = "akserver.fixed";

    static final int MAX_TRANSACTION_RETRY_COUNT = 10;

    private ConfigurationService configService;

    private static int instanceCount = 0;

    private final SortedMap<String, SchemaNode> schemaMap = new TreeMap<String, SchemaNode>();

    private final AtomicReference<Persistit> dbRef = new AtomicReference<Persistit>();

    private int volumeOffsetCounter = 0;

    private final Map<String, TreeLink> schemaLinkMap = new HashMap<String, TreeLink>();

    private final Map<String, TreeLink> statusLinkMap = new HashMap<String, TreeLink>();

    private TableStatusCache tableStatusCache;

    private final TreeServiceMXBean bean = new TreeServiceMXBean() {
        @Override
        public void flushAll() throws Exception {
            final Persistit db = dbRef.get();
            try {
                db.flush();
                db.copyBackPages();
            } catch (Exception e) {
                LOG.error("flush failed", e);
                throw e;
            }
        }
    };

    static class SchemaNode {
        final Pattern pattern;
        final String volumeString;

        private SchemaNode(final Pattern pattern, final String volumeString) {
            this.pattern = pattern;
            this.volumeString = volumeString;
        }

        /**
         * @return the tree pattern
         */
        public Pattern getPattern() {
            return pattern;
        }

        /**
         * @return the volumeSpec
         */
        public String getVolumeString() {
            return volumeString;
        }
    }

    public synchronized void start() throws Exception {
        configService = ServiceManagerImpl.get().getConfigurationService();
        assert getDb() == null;
        // TODO - remove this when sure we don't need it
        ++instanceCount;
        assert instanceCount == 1 : instanceCount;
        final Properties properties = configService.getModuleConfiguration(
                PERSISTIT_MODULE_NAME).getProperties();
        //
        // This section modifies the properties gotten from the
        // default configuration plus chunkserver.properties. It
        //
        // (a) copies akserver.datapath to datapath
        // (b) sets the buffersize property if null
        // (c) sets the buffercount property if null.
        //
        // Copies the akserver.datapath property to the Persistit properties
        // set.
        // This allows Persistit to perform substitution of ${datapath} with
        // the server-specified home directory.
        //
        final String datapath = configService.getProperty("akserver."
                + DATAPATH_PROP_NAME, DEFAULT_DATAPATH);
        properties.setProperty(DATAPATH_PROP_NAME, datapath);
        ensureDirectoryExists(datapath, false);

        // Note - this is an akserver property, not a persistit property.
        // Is used by unit tests to limit the size of buffer pool -
        // for startup/shutdown speed.
        //
        final boolean isFixedAllocation = "true".equals(configService
                .getProperty(FIXED_ALLOCATION_PROPERTY_NAME, "false"));

        // Get the configured buffer size:
        // Default is 16K. Can be overridden with
        //
        // persistit.buffersize=8K
        //
        // for example.
        if (!properties.containsKey(BUFFER_SIZE_PROP_NAME)) {
            properties.setProperty(BUFFER_SIZE_PROP_NAME,
                    String.valueOf(DEFAULT_BUFFER_SIZE));
        }
        //
        // Now compute the actual allocation of buffers
        // of that size. The bufferCount method computes
        // an allocation based on heap size.
        //
        final int bufferSize = Integer.parseInt(properties
                .getProperty(BUFFER_SIZE_PROP_NAME));
        final String bufferCountPropString = BUFFER_COUNT_PROP_NAME
                + bufferSize;
        if (!properties.containsKey(bufferCountPropString)) {
            properties.setProperty(bufferCountPropString,
                    String.valueOf(bufferCount(bufferSize, isFixedAllocation)));
        }
        //
        // Now we're ready to create the Persistit instance.
        // Note that the Volume specifications will substitute
        // ${buffersize}, so that property must be valid.
        //
        Persistit db = new Persistit();
        dbRef.set(db);

        tableStatusCache = new TableStatusCache(db, this);
        tableStatusCache.register();

        db.setPersistitLogger(new PersistitSlf4jAdapter(LOG));
        db.initialize(properties);
        buildSchemaMap();

        if (LOG.isDebugEnabled()) {
            LOG.debug("PersistitStore datapath={} {} k_buffers={}", new Object[] {
                    db.getProperty("datapath"),
                    bufferSize / 1024,
                    db.getProperty("buffer.count." + bufferSize)
            });
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
        final long allocation = (long) ((AkServerUtil.availableMemory() - MEMORY_RESERVATION) * PERSISTIT_ALLOCATION_FRACTION);
        final int allocationPerBuffer = (int) (bufferSize * 1.5);
        return Math.max(512, (int) (allocation / allocationPerBuffer));
    }

    public synchronized void stop() throws Exception {
        Persistit db = getDb();
        if (db != null) {
            db.shutdownGUI();
            db.close();
            dbRef.set(null);
        }
        schemaLinkMap.clear();
        statusLinkMap.clear();
        // TODO - remove this when sure we don't need it
        --instanceCount;
        assert instanceCount == 0 : instanceCount;
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
        return dbRef.get();
    }

    @Override
    public void crash() throws Exception {
        Persistit db = getDb();
        if (db != null) {
            db.shutdownGUI();
            db.crash();
            dbRef.set(null);
        }
        --instanceCount;
        assert instanceCount == 0 : instanceCount;
    }

    @Override
    public Exchange getExchange(final Session session, final TreeLink link)
            throws PersistitException {
        final TreeCache cache = populateTreeCache(link);
        Tree tree = cache.getTree();
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
        return getDb().getTransaction();
    }

    @Override
    public long getTimestamp(final Session session) {
        return getDb().getTransaction().getTimestamp();
    }

    @Override
    public void checkpoint() {
        getDb().checkpoint();
    }

    @Override
    public TableStatusCache getTableStatusCache() {
        return tableStatusCache;
    }

    @Override
    public void visitStorage(final Session session, final TreeVisitor visitor,
            final String treeName) throws Exception {
        Persistit db = getDb();
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
    public boolean isContainer(final Exchange exchange, final TreeLink link)
            throws PersistitException {
        final TreeCache treeCache = populateTreeCache(link);
        return exchange.getVolume().equals(treeCache.getTree().getVolume());
    }

    @Override
    public int aisToStore(final TreeLink link, final int tableId)
            throws PersistitException {
        final TreeCache cache = populateTreeCache(link);
        int offset = cache.getTableIdOffset();
        if (offset < 0) {
            offset = tableIdOffset(link);
            cache.setTableIdOffset(offset);
        }
        return tableId - offset;
    }

    @Override
    public int storeToAis(final TreeLink link, final int tableId)
            throws PersistitException {
        final TreeCache cache = populateTreeCache(link);
        int offset = cache.getTableIdOffset();
        if (offset < 0) {
            offset = tableIdOffset(link);
            cache.setTableIdOffset(offset);
        }
        return tableId + offset;
    }

    @Override
    public int storeToAis(final Volume volume, final int tableId) {
        final int offset = tableIdOffset(volume);
        return tableId + offset;
    }

    private TreeCache populateTreeCache(final TreeLink link)
            throws PersistitException {
        TreeCache cache = (TreeCache) link.getTreeCache();
        if (cache == null) {
            Volume volume = mappedVolume(link.getSchemaName(),
                    link.getTreeName());
            final Tree tree = volume.getTree(link.getTreeName(), true);
            cache = new TreeCache(tree);
            link.setTreeCache(cache);
        }
        return cache;
    }

    private int tableIdOffset(final TreeLink link) throws PersistitException {
        final Volume volume = mappedVolume(link.getSchemaName(),
                SCHEMA_TREE_NAME);
        return tableIdOffset(volume);
    }

    private int tableIdOffset(final Volume volume) {
        while (volume.getAppCache() == null) {
            synchronized (this) {
                if (volume.getAppCache() == null) {
                    volume.setAppCache(Integer.valueOf(volumeOffsetCounter));
                    volumeOffsetCounter += MAX_TABLES_PER_VOLUME;
                }
            }
        }
        return ((Integer) volume.getAppCache()).intValue();
    }

    public synchronized Volume mappedVolume(final String schemaName,
            final String treeName) throws PersistitException {
        try {
            final String vstring = volumeForTree(schemaName, treeName);
            final Volume volume = getDb().loadVolume(vstring);
            if (volume.getAppCache() == null) {
                volume.setAppCache(Integer.valueOf(volumeOffsetCounter));
                volumeOffsetCounter += MAX_TABLES_PER_VOLUME;
                final Exchange exchange = new Exchange(getDb(), volume, STATUS_TREE_NAME, true);
                tableStatusCache.loadOneVolume(exchange);
            }
            return volume;
        } catch (InvalidVolumeSpecificationException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<Exchange> exchangeList(final Session session, final Tree tree) {
        Map<Tree, List<Exchange>> map = session.get(EXCHANGE_MAP);
        List<Exchange> list;
        if (map == null) {
            map = new HashMap<Tree, List<Exchange>>();
            session.put(EXCHANGE_MAP, map);
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

    String volumeForTree(final String schemaName, final String treeName)
            throws InvalidVolumeSpecificationException {
        SchemaNode defaultSchemaNode = null;
        final String concatenatedName = schemaName + "/" + treeName;
        final Persistit db = getDb();
        for (final Entry<String, SchemaNode> entry : schemaMap.entrySet()) {
            if (".default".equals(entry.getKey())) {
                defaultSchemaNode = entry.getValue();
            } else {
                final SchemaNode node = entry.getValue();
                if (node.getPattern().matcher(concatenatedName).matches()) {
                    String vs = entry.getValue().getVolumeString();
                    return substitute(vs, schemaName, treeName);
                }
            }
        }
        if (defaultSchemaNode != null) {
            String vs = defaultSchemaNode.getVolumeString();
            return substitute(vs, schemaName, null);
        }
        return null;
    }

    public TreeLink treeLink(final String schemaName, final String treeName) {
        final Map<String, TreeLink> map = treeName == STATUS_TREE_NAME ? statusLinkMap
                : schemaLinkMap;
        TreeLink link;
        synchronized (map) {
            link = map.get(schemaName);
            if (link == null) {
                link = new TreeLink() {
                    TreeCache cache;

                    @Override
                    public String getSchemaName() {
                        return schemaName;
                    }

                    @Override
                    public String getTreeName() {
                        return treeName;
                    }

                    @Override
                    public void setTreeCache(TreeCache cache) {
                        this.cache = cache;
                    }

                    @Override
                    public TreeCache getTreeCache() {
                        return cache;
                    }

                };
                map.put(schemaName, link);
            }
        }
        return link;
    }

    void buildSchemaMap() {
        final Properties properties = configService.getModuleConfiguration(
                "akserver").getProperties();
        for (final Entry<Object, Object> entry : properties.entrySet()) {
            final String name = (String) entry.getKey();
            final String value = (String) entry.getValue();
            if (name.startsWith(TREESPACE)) {
                final String tsName = name.substring(TREESPACE.length());
                final String[] parts = value.split(":");
                boolean valid = true;
                final StringBuilder sb = new StringBuilder();
                if (parts.length > 1) {
                    valid &= parseSchemaExpr(parts[0], sb);
                } else {
                    valid = false;
                }
                if (!valid) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Invalid treespace property " + entry
                                + " ignored");
                    }
                    continue;
                }
                if (schemaMap.containsKey(tsName)) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Invalid duplicate treespace property "
                                + entry + " ignored");
                    }
                    continue;
                }
                final Pattern pattern = Pattern.compile(sb.toString());
                final String vstring = value.substring(parts[0].length() + 1);
                try {
                    // Test for value Volume specification
                    // Done here during startup so that any configuration error
                    // is revealed early. Note that this intentionally replaces
                    // ${schema} and ${tree} substrings with "SCHEMA" and
                    // "TREE". With this values the string will pass validation.
                    // These are not the real values that will be substituted
                    // when the volume specification is actually used - they are
                    // merely syntactically valid.
                    new VolumeSpecification(substitute(vstring, SCHEMA, TREE));
                } catch (InvalidVolumeSpecificationException e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Invalid volumespecification in property "
                                + entry + ": " + e);
                    }
                    continue;
                }
                schemaMap.put(tsName, new SchemaNode(pattern, vstring));
            }
        }
    }

    SortedMap<String, SchemaNode> getSchemaMap() {
        return schemaMap;
    }

    private boolean parseSchemaExpr(final String expr, final StringBuilder sb) {
        if (expr.length() == 0) {
            return false;
        }
        for (int i = 0; i < expr.length(); i++) {
            final char c = expr.charAt(i);
            if (c == '*') {
                sb.append(".*");
            } else if (c == '?') {
                sb.append(".");
            } else if (Character.isLetter(c)) {
                sb.append(c);
            } else {
                sb.append('\\');
                sb.append(c);
            }
        }
        return true;
    }

    private String substitute(final String vs, final String schemaName,
            final String treeName) {
        final Persistit db = dbRef.get();
        final Properties props = new Properties(db.getProperties());
        if (schemaName != null) {
            props.put(SCHEMA, schemaName);
        }
        if (treeName != null) {
            props.put(TREE, treeName);
        }
        return db.substituteProperties(vs, props);
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("TreeService", bean, TreeServiceMXBean.class);
    }
}
