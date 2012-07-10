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

import com.akiban.server.PersistitAccumulatorTableStatusCache;
import com.akiban.server.TableStatusCache;
import com.akiban.server.collation.CString;
import com.akiban.server.collation.CStringKeyCoder;
import com.akiban.server.error.ConfigurationPropertiesLoadException;
import com.akiban.server.error.InvalidVolumeException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.google.inject.Inject;
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
    implements TreeService, Service<TreeService>, JmxManageable
{

    private final static Session.Key<Map<Tree, List<Exchange>>> EXCHANGE_MAP = Session.Key
            .named("exchangemap");

    private static final Logger LOG = LoggerFactory
            .getLogger(TreeServiceImpl.class.getName());

    private static final String PERSISTIT_MODULE_NAME = "persistit.";

    private static final String DATAPATH_PROP_NAME = "datapath";

    private static final String BUFFER_SIZE_PROP_NAME = "buffersize";

    private static final Session.Key<Volume> TEMP_VOLUME = Session.Key.named("TEMP_VOLUME");

    // Must be one of 1024, 2048, 4096, 8192, 16384:
    static final int DEFAULT_BUFFER_SIZE = 16384;

    private final ConfigurationService configService;

    private static int instanceCount = 0;

    private final SortedMap<String, SchemaNode> schemaMap = new TreeMap<String, SchemaNode>();

    private final AtomicReference<Persistit> dbRef = new AtomicReference<Persistit>();

    private int volumeOffsetCounter = 0;

    private final Map<String, TreeLink> schemaLinkMap = new HashMap<String, TreeLink>();

    private final SessionService sessionService;

    private TableStatusCache tableStatusCache;

    @Inject
    public TreeServiceImpl(SessionService sessionService, ConfigurationService configService) {
        this.sessionService = sessionService;
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

        db.setPersistitLogger(new Slf4jAdapter(LOG));
        try {
            db.initialize(properties);
        } catch (PersistitException e1) {
            throw new PersistitAdapterException(e1);
        }
        buildSchemaMap();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
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
        //
        // Copies the akserver.datapath property to the Persistit properties
        // set. This allows Persistit to perform substitution of ${datapath}
        // with the server-specified home directory.
        //
        // Sets the property named "buffersize" so that the volume
        // specifications can use the substitution syntax ${buffersize}.
        //
        final String datapath = configService.getProperty("akserver." + DATAPATH_PROP_NAME);
        properties.setProperty(DATAPATH_PROP_NAME, datapath);
        ensureDirectoryExists(datapath, false);

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
            schemaLinkMap.clear();
            // TODO - remove this when sure we don't need it
            --instanceCount;
            assert instanceCount == 0 : instanceCount;
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
            LOG.error("flush failed", e);
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
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public Key getKey(Session session) {
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
    public int aisToStore(final TreeLink link, final int tableId) {
        try {
            final TreeCache cache = populateTreeCache(link);
            int offset = cache.getTableIdOffset();
            if (offset < 0) {
                offset = tableIdOffset(link);
                cache.setTableIdOffset(offset);
            }
            return tableId - offset;
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public int storeToAis(final TreeLink link, final int tableId) {
        try {
            final TreeCache cache = populateTreeCache(link);
            int offset = cache.getTableIdOffset();
            if (offset < 0) {
                offset = tableIdOffset(link);
                cache.setTableIdOffset(offset);
            }
            return tableId + offset;
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public int storeToAis(final Volume volume, final int tableId) {
        final int offset = tableIdOffset(volume);
        return tableId + offset;
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
            }
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
            map = new HashMap<Tree, List<Exchange>>();
            session.put(EXCHANGE_MAP, map);
            list = new ArrayList<Exchange>();
            map.put(tree, list);
        } else {
            list = map.get(tree);
            if (list == null) {
                list = new ArrayList<Exchange>();
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
        final Map<String, TreeLink> map = schemaLinkMap;
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

    @Override
    public String getDataPath() {
        return getDb().getProperty("datapath");
    }

    void buildSchemaMap() {
        final Properties properties = configService.deriveProperties("akserver.");
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
    
    private TableStatusCache createTableStatusCache() {
        //return new MemoryOnlyTableStatusCache();
        return new PersistitAccumulatorTableStatusCache(this);
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("TreeService", bean, TreeServiceMXBean.class);
    }
}
