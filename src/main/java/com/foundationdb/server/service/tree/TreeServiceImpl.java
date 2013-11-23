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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.server.PersistitAccumulatorTableStatusCache;
import com.foundationdb.server.TableStatusCache;
import com.foundationdb.server.error.ConfigurationPropertiesLoadException;
import com.foundationdb.server.error.PersistitAdapterException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.server.service.session.Session;
import com.google.inject.Inject;
import com.persistit.Configuration;
import com.persistit.Configuration.BufferPoolConfiguration;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import com.persistit.logging.Slf4jAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeServiceImpl implements TreeService, Service, JmxManageable
{
    private static final Logger LOG = LoggerFactory.getLogger(TreeServiceImpl.class);
    private static final Session.Key<Map<Tree, List<Exchange>>> EXCHANGE_MAP = Session.Key.named("exchangemap");

    // Persistit properties
    private static final String PERSISTIT_MODULE_NAME = "persistit.";
    private static final String DATA_PATH_PROP_NAME = "datapath";
    // TreeService properties
    private static final String TMP_DIR_PROP_NAME = "fdbsql.tmp_dir";
    private static final String DATA_VOLUME_PROP_NAME = "fdbsql.persistit.data_volume";

    private final TreeServiceMXBean bean = new TreeServiceMXBean() {
        @Override
        public void flushAll() throws Exception {
            TreeServiceImpl.this.flushAll();
        }
    };

    private final ConfigurationService configService;

    // Set/cleared in start/stop
    private static int instanceCount = 0;
    private Persistit db;
    private Volume dataVolume;
    private TableStatusCache tableStatusCache;


    @Inject
    public TreeServiceImpl(ConfigurationService configService) {
        this.configService = configService;
    }

    @Override
    public synchronized void start() {
        assert (db == null);
        assert (instanceCount == 0) : instanceCount;

        final Properties properties;
        final String dataVolumeName;
        try {
            properties = setupPersistitProperties(configService);
            dataVolumeName = configService.getProperty(DATA_VOLUME_PROP_NAME);
        } catch (FileNotFoundException e) {
            throw new ConfigurationPropertiesLoadException ("Persistit Properties", e.getMessage());
        }

        Persistit tmpDB = new Persistit();
        tmpDB.setPersistitLogger(new Slf4jAdapter(LOG));
        try {
            tmpDB.setConfiguration(new Configuration(properties));
            tmpDB.initialize();
        } catch (PersistitException e1) {
            throw new PersistitAdapterException(e1);
        }

        ++instanceCount;
        this.db = tmpDB;
        this.dataVolume = db.getVolume(dataVolumeName);
        this.tableStatusCache = new PersistitAccumulatorTableStatusCache(this);

        if(this.dataVolume == null) {
            stop();
            throw new IllegalArgumentException("No volume named: " + dataVolumeName);
        }

        if(LOG.isDebugEnabled()) {
            int pageSize = dataVolume.getPageSize();
            BufferPoolConfiguration bufferConfig = db.getConfiguration().getBufferPoolMap().get(pageSize);
            LOG.debug("dataPath={} bufferSize={} maxBuffers={} maxMemory={}",
                      new Object[]{
                          dataVolume.getPath(),
                          pageSize,
                          bufferConfig.getMaximumCount(),
                          bufferConfig.getMaximumMemory()
                      });
        }
    }

    /** Copy, and strip, "{@value #PERSISTIT_MODULE_NAME}" properties to a new Properties object. */
    static Properties setupPersistitProperties(ConfigurationService configService) throws FileNotFoundException {
        final Properties properties = configService.deriveProperties(PERSISTIT_MODULE_NAME);
        final String datapath = properties.getProperty(DATA_PATH_PROP_NAME);
        ensureDirectoryExists(datapath, false);
        // Copied the fdbsql.tmp_dir property to the Persistit tmpvoldir
        // The latter is used for temporary Persistit volumes used for sorting.
        final String tmpPath = configService.getProperty(TMP_DIR_PROP_NAME);
        properties.setProperty(Configuration.TEMPORARY_VOLUME_DIR_PROPERTY_NAME, tmpPath);
        ensureDirectoryExists(tmpPath, false);
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

    @Override
    public synchronized void stop() {
        assert (db != null);
        assert (instanceCount == 1) : instanceCount;
        db.shutdownGUI();
        try {
            db.close();
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
        dataVolume = null;
        tableStatusCache = null;
        db = null;
        --instanceCount;
    }

    @Override
    public Persistit getDb() {
        return db;
    }

    @Override
    public Collection<String> getAllTreeNames() {
        try {
            return Arrays.asList(dataVolume.getTreeNames());
        } catch(PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public synchronized void crash() {
        assert (db != null);
        assert (instanceCount == 1) : instanceCount;
        db.shutdownGUI();
        db.crash();
        dataVolume = null;
        tableStatusCache = null;
        db = null;
        --instanceCount;
    }

    private void flushAll() throws Exception {
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
            final Tree tree = populateTreeCache(link);
            final Exchange exchange = getExchange(session, tree);
            exchange.setAppCache(link);
            return exchange;
        } catch (PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
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
        exchange.setAppCache(null);
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
    public Tree populateTreeCache(final TreeLink link) throws PersistitException {
        Tree tree = link.getTreeCache();
        if (tree == null || !tree.isValid()) {
            tree = dataVolume.getTree(link.getTreeName(), true);
            link.setTreeCache(tree);
        }
        return tree;
    }

    /** Provide a list of Exchange instances already created for a particular Tree. */
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
    public boolean treeExists(String treeName) {
        try {
            final Tree tree = dataVolume.getTree(treeName, false);
            return tree != null;
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public TreeLink treeLink(final String treeName) {
        return new TreeLink() {
            private Tree cache;

            @Override
            public String getTreeName() {
                return treeName;
            }

            @Override
            public void setTreeCache(Tree cache) {
                this.cache = cache;
            }

            @Override
            public Tree getTreeCache() {
                return cache;
            }
        };
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("TreeService", bean, TreeServiceMXBean.class);
    }
}
