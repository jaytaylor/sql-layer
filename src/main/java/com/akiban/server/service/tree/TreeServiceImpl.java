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
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.AkServerUtil;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.Volume;
import com.persistit.VolumeSpecification;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.PersistitException;

public class TreeServiceImpl implements TreeService, Service<TreeService> {

    private final static int MEGA = 1024 * 1024;

    private static final Logger LOG = LoggerFactory.getLogger(TreeServiceImpl.class
            .getName());

    private static final String SERVER_MODULE_NAME = "cserver";

    private static final String PERSISTIT_MODULE_NAME = "persistit";

    private static final String DATAPATH_PROP_NAME = "datapath";

    private static final String BUFFER_SIZE_PROP_NAME = "buffersize";

    private static final String BUFFER_COUNT_PROP_NAME = "buffer.count.";

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

    private ConfigurationService configService;

    private final static AtomicInteger INSTANCE_COUNT = new AtomicInteger();

    private final SortedMap<String, SchemaNode> schemaMap = new TreeMap<String, SchemaNode>();

    private Persistit db;

    private int volumeOffsetCounter = 0;

    private final Map<Volume, Integer> translationMap = new WeakHashMap<Volume, Integer>();

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
        final String bufferCountPropString = BUFFER_COUNT_PROP_NAME
                + bufferSize;
        if (!properties.contains(bufferCountPropString)) {
            properties.setProperty(bufferCountPropString,
                    String.valueOf(bufferCount(bufferSize, isFixedAllocation)));
        }
        //
        // Now we're ready to create the Persistit instance.
        //
        db = new Persistit();
        db.setPersistitLogger(new PersistitSlf4jAdapter(LOG));
        db.initialize(properties);
        buildSchemaMap();

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
        final long allocation = (long) ((AkServerUtil.availableMemory() - MEMORY_RESERVATION) * PERSISTIT_ALLOCATION_FRACTION);
        final int allocationPerBuffer = (int) (bufferSize * 1.5);
        return Math.max(512, (int) (allocation / allocationPerBuffer));
    }

    public synchronized void stop() throws Exception {
        if (db != null) {
            db.shutdownGUI();
            db.close();
            db = null;
        }
        // TODO - remove this when sure we don't need it
        assert INSTANCE_COUNT.decrementAndGet() == 0;
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
        return db.getTransaction();
    }

    @Override
    public long getTimestamp(final Session session) {
        return db.getTransaction().getTimestamp();
    }

    @Override
    public void visitStorage(final Session session, final TreeVisitor visitor,
            final String treeName) throws Exception {
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
        return exchange.getVolume().equals(
                 treeCache.getTree().getVolume());
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

    private synchronized int tableIdOffset(final Volume volume) {
        Integer offset = translationMap.get(volume);
        if (offset == null) {
            offset = Integer.valueOf(volumeOffsetCounter);
            translationMap.put(volume, offset);
            volumeOffsetCounter += MAX_TABLES_PER_VOLUME;
        }
        return offset.intValue();
    }

    public Volume mappedVolume(final String schemaName, final String treeName)
            throws PersistitException {
        try {
            final String vstring = volumeForTree(schemaName, treeName);
            final Volume volume = db.loadVolume(vstring);
            if (SCHEMA_TREE_NAME.equals(treeName)) {
                tableIdOffset(volume);
            } else {
                final Volume schemaVolume = mappedVolume(schemaName, SCHEMA_TREE_NAME);
                int schemaTableId = tableIdOffset(schemaVolume);
                synchronized(this) {
                    translationMap.put(volume, schemaTableId);
                }
            }
            return volume;
        } catch (InvalidVolumeSpecificationException e) {
            throw new IllegalStateException(e);
        }
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

    String volumeForTree(final String schemaName, final String treeName)
            throws InvalidVolumeSpecificationException {
        SchemaNode defaultSchemaNode = null;
        final String concatenatedName = schemaName + "/" + treeName;
        for (final Entry<String, SchemaNode> entry : schemaMap.entrySet()) {
            if (".default".equals(entry.getKey())) {
                defaultSchemaNode = entry.getValue();
            } else {
                final SchemaNode node = entry.getValue();
                if (node.getPattern().matcher(concatenatedName).matches()) {
                    String vs = entry.getValue().getVolumeString();
                    db.setProperty(SCHEMA, schemaName);
                    db.setProperty(TREE, treeName);
                    String vsFinal = db.substituteProperties(vs,
                            db.getProperties());
                    return vsFinal;
                }
            }
        }
        if (defaultSchemaNode != null) {
            String vs = defaultSchemaNode.getVolumeString();
            db.setProperty(SCHEMA, schemaName);
            String vsFinal = db.substituteProperties(vs, db.getProperties());
            return vsFinal;
        }
        return null;
    }

    void buildSchemaMap() {
        final Properties properties = configService.getModuleConfiguration(
                SERVER_MODULE_NAME).getProperties();
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
                    new VolumeSpecification(vstring);
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
}
