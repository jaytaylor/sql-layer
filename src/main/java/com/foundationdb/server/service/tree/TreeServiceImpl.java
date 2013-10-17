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

package com.foundationdb.server.service.tree;

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

import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.server.PersistitAccumulatorTableStatusCache;
import com.foundationdb.server.TableStatusCache;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.ConfigurationPropertiesLoadException;
import com.foundationdb.server.error.InvalidVolumeException;
import com.foundationdb.server.error.PersistitAdapterException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.server.service.session.Session;
import com.google.inject.Inject;
import com.persistit.Configuration;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.Volume;
import com.persistit.VolumeSpecification;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.PersistitException;
import com.persistit.logging.Slf4jAdapter;

public class TreeServiceImpl
    implements TreeService, Service, JmxManageable
{

    private final static Session.Key<Map<Tree, List<Exchange>>> EXCHANGE_MAP = Session.Key
            .named("exchangemap");

    private static final String PERSISTIT_MODULE_NAME = "persistit.";

    private static final String DATAPATH_PROP_NAME = "datapath";
    
    private static final String TEMPDIR_NAME = "fdbsql.tmp_dir";

    private static final String BUFFER_SIZE_PROP_NAME = "buffersize";
    
    private static final String COLLATION_PROP_NAME = "fdbsql.collation";

    private static final Session.Key<Volume> TEMP_VOLUME = Session.Key.named("TEMP_VOLUME");

    // Must be one of 1024, 2048, 4096, 8192, 16384:
    static final int DEFAULT_BUFFER_SIZE = 16384;

    private final ConfigurationService configService;

    private static int instanceCount = 0;

    private final SortedMap<String, SchemaNode> schemaMap = new TreeMap<>();

    private final AtomicReference<Persistit> dbRef = new AtomicReference<>();

    private int volumeOffsetCounter = 0;

    private TableStatusCache tableStatusCache;

    @Inject
    public TreeServiceImpl(ConfigurationService configService) {
        this.configService = configService;
    }

    private final TreeServiceMXBean bean = new TreeServiceMXBean() {
        @Override
        public void flushAll() throws Exception {
            TreeServiceImpl.this.flushAll();
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

    public synchronized void start() {
        assert getDb() == null;
        /*
         * TODO:
         * Remove when AkCollatorFactory becomes a service.
         * Temporary bridge to get the fdbsql.collation property AkCollatorFactory
         */
        AkCollatorFactory.setCollationMode(configService.getProperty(COLLATION_PROP_NAME));
        // TODO - remove this when sure we don't need it
        ++instanceCount;
        assert instanceCount == 1 : instanceCount;
        Properties properties;
        int bufferSize;
        try {
            properties = setupPersistitProperties(configService);
            bufferSize = Integer.parseInt(properties.getProperty(BUFFER_SIZE_PROP_NAME));
        } catch (FileNotFoundException e) {
            throw new ConfigurationPropertiesLoadException ("Persistit Properties", e.getMessage());
        }

        Persistit db = new Persistit();
        dbRef.set(db);

        tableStatusCache = createTableStatusCache();

        db.setPersistitLogger(new Slf4jAdapter(logger));
        try {
            db.initialize(properties);
        } catch (PersistitException e1) {
            throw new PersistitAdapterException(e1);
        }
        buildSchemaMap();

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "PersistitStore datapath={} {} k_buffers={}",
                    new Object[] { db.getProperty("datapath"),
                            bufferSize / 1024,
                            db.getProperty("buffer.count." + bufferSize) });
        }
    }

    static Properties setupPersistitProperties(
            final ConfigurationService configService)
            throws FileNotFoundException {
        //
        // Copies all the Persistit properties to a local Properties object.
        // Note that this strips the prefix "persistit." from each key, for
        // example, if the configuration file specifies
        //
        // persistit.appendonly=true
        //
        // then the corresponding key created by getModuleConfiguration will be
        // just "appendonly".
        //
        final Properties properties = configService.deriveProperties(PERSISTIT_MODULE_NAME);

        final String datapath = properties.getProperty(DATAPATH_PROP_NAME);
        ensureDirectoryExists(datapath, false);

        //
        // Copied the fdbsql.tmp_dir property to the Persistit tmpvoldir
        // The latter is used for temporary Persistit volumes used for sorting.
        final String tmpPath = configService.getProperty(TEMPDIR_NAME);
        properties.setProperty(Configuration.TEMPORARY_VOLUME_DIR_PROPERTY_NAME, tmpPath);
        ensureDirectoryExists(tmpPath, false);
        
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

        return properties;
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

    public void stop() {
        Persistit db = getDb();
        if (db != null) {
            db.shutdownGUI();
            try {
                db.close();
            } catch (PersistitException e) {
                throw new PersistitAdapterException(e);
            }
            dbRef.set(null);
        }
        synchronized (this) {
            // TODO - remove this when sure we don't need it
            --instanceCount;
            assert instanceCount == 0 : instanceCount;
        }
    }

    @Override
    public Persistit getDb() {
        return dbRef.get();
    }

    @Override
    public void crash() {
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
    public void flushAll() throws Exception {
        final Persistit db = dbRef.get();
        try {
            db.flush();
            db.copyBackPages();
        } catch (PersistitException e) {
            logger.error("flush failed", e);
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public Exchange getExchange(final Session session, final TreeLink link) {
        try {
            final TreeCache cache = populateTreeCache(link);
            final Tree tree = cache.getTree();
            return getExchange(session, tree);
        } catch (PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    public Key getKey() {
        return new Key(getDb());
    }

    @Override
    public Exchange getExchange(final Session session, final Tree tree) {
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
        if (exchange.getTree().isValid()) {
            final List<Exchange> list = exchangeList(session, exchange.getTree());
            list.add(exchange);
        } else {
            Map<Tree, List<Exchange>> map = session.get(EXCHANGE_MAP);
            if (map != null) {
                map.remove(exchange.getTree());
            }
        }
    }

    @Override
    public Transaction getTransaction(final Session session) {
        return getDb().getTransaction();
    }

    @Override
    public void checkpoint()  throws PersistitException {
        getDb().checkpoint();
    }

    @Override
    public TableStatusCache getTableStatusCache() {
        return tableStatusCache;
    }

    @Override
    public void visitStorage(final Session session, final TreeVisitor visitor,
            final String treeName) throws PersistitException {
        Persistit db = getDb();
        final Volume sysVol = db.getSystemVolume();
        for (final Volume volume : db.getVolumes()) {
            if (volume != sysVol) {
                final Tree tree = volume.getTree(treeName, false);
                if (tree != null) {
                    final Exchange exchange = getExchange(session, tree);
                    try {
                        visitor.visit(exchange);
                    } finally {
                        releaseExchange(session, exchange);
                    }
                }
            }
        }
    }

    @Override
    public boolean isContainer(final Exchange exchange, final TreeLink link){
        try {
            final TreeCache treeCache = populateTreeCache(link);
            return exchange.getVolume().equals(treeCache.getTree().getVolume());
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public TreeCache populateTreeCache(final TreeLink link) throws PersistitException {
        TreeCache cache = (TreeCache) link.getTreeCache();
        if (cache == null || !cache.getTree().isValid()) {
            Volume volume = mappedVolume(link.getSchemaName(), link.getTreeName());
            final Tree tree = volume.getTree(link.getTreeName(), true);
            cache = new TreeCache(tree);
            link.setTreeCache(cache);
        }
        return cache;
    }

    public synchronized Volume mappedVolume(final String schemaName,
            final String treeName) throws PersistitException {
        try {
            final String vstring = volumeForTree(schemaName, treeName);
            final Volume volume = getDb().loadVolume(vstring);
            return volume;
        } catch (InvalidVolumeSpecificationException e) {
            throw new InvalidVolumeException (e);
        } catch (Exception e) {
            throw new PersistitException(e);
        }
    }

    /**
     * Provide a list of Exchange instances already created for a particular
     * Tree.
     * 
     * @param session
     * @param tree
     * @return
     */
    List<Exchange> exchangeList(final Session session, final Tree tree) {
        Map<Tree, List<Exchange>> map = session.get(EXCHANGE_MAP);
        List<Exchange> list;
        if (map == null) {
            map = new HashMap<>();
            session.put(EXCHANGE_MAP, map);
            list = new ArrayList<>();
            map.put(tree, list);
        } else {
            list = map.get(tree);
            if (list == null) {
                list = new ArrayList<>();
                map.put(tree, list);
            } else {
                if (!list.isEmpty()
                        && !list.get(list.size() - 1).getTree().isValid()) {
                    //
                    // The Tree on which this list of cached Exchanges is
                    // based was deleted. Need to clear the list. Further,
                    // remove the obsolete Tree object from the Map and replace
                    // it with the new valid Tree.
                    list.clear();
                    map.remove(tree);
                    map.put(tree, list);
                }
            }
        }
        return list;
    }

    @Override
    public String volumeForTree(final String schemaName, final String treeName) {
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

    @Override
    public boolean treeExists(final String schemaName, final String treeName) {
        try {
            final Volume volume = mappedVolume(schemaName, treeName);
            final Tree tree = volume.getTree(treeName, false);
            return tree != null;
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    public TreeLink treeLink(final String schemaName, final String treeName) {
        return new TreeLink() {
            private TreeCache cache;

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
    }

    @Override
    public String getDataPath() {
        return getDb().getProperty("datapath");
    }
    
    @Override
    public Key createKey() {
        return new Key(getDb());
    }

    void buildSchemaMap() {
        final Properties properties = configService.deriveProperties("fdbsql.");
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
                    if (logger.isErrorEnabled()) {
                        logger.error("Invalid treespace property " + entry
                                + " ignored");
                    }
                    continue;
                }
                if (schemaMap.containsKey(tsName)) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Invalid duplicate treespace property "
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
                    if (logger.isErrorEnabled()) {
                        logger.error("Invalid volumespecification in property "
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
    
    private TableStatusCache createTableStatusCache() {
        //return new MemoryOnlyTableStatusCache();
        return new PersistitAccumulatorTableStatusCache(this);
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("TreeService", bean, TreeServiceMXBean.class);
    }
}
