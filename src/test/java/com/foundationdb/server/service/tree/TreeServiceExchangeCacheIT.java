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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.store.format.PersistitStorageDescription;
import com.foundationdb.server.test.it.PersistitITBase;
import org.junit.Before;
import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TreeServiceExchangeCacheIT extends PersistitITBase
{
    private static final int MAX_TREE_CACHE = 6;
    private static final int MAX_EXCHANGE_CACHE = 3;

    private TreeServiceImpl treeService;

    private static final String identifier = "test";

    @Before
    public void setTreeService() {
        treeService = (TreeServiceImpl)treeService();
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> props = new HashMap<>( super.startupConfigProperties() );
        props.put(TreeServiceImpl.MAX_TREE_CACHE_PROP_NAME, Integer.toString(MAX_TREE_CACHE));
        props.put(TreeServiceImpl.MAX_EXCHANGE_CACHE_PROP_NAME, Integer.toString(MAX_EXCHANGE_CACHE));
        return props;
    }


    /**
     * Tree being removed will cause cache clear on release.
     * However, Trees aren't normally removed as they are non-transactional in < Persistit 3.3.0.
     */
    @Test
    public void invalidTreeClearsCache() throws Exception {
        final Exchange ex1 = treeService.getExchange(session(), new TestLink("schema", "someTree"));
        final Tree tree = ex1.getTree();
        treeService.releaseExchange(session(), ex1);
        assertFalse(treeService.exchangeQueue(session(), ex1.getTree()).isEmpty());
        final Exchange ex2 = treeService.getExchange(session(), new TestLink("schema", "someTree"));
        final Exchange ex3 = treeService.getExchange(session(), new TestLink("schema", "someTree"));
        treeService.releaseExchange(session(), ex3);
        assertEquals("cached exchange count", 1, getCachedExchangeCount(tree));
        ex2.removeTree();
        treeService.releaseExchange(session(), ex2);
        assertEquals("cached exchange queue", null, treeService.exchangeQueue(session(), tree));
    }

    /** As {@link #invalidTreeClearsCache} but Store level removal also busts cache. */
    @Test
    public void storeRemoveTreeClearsCache() {
        PersistitStorageDescription desc = createDescription("tree");
        final Exchange ex1 = treeService.getExchange(session(), desc);
        final Exchange ex2 = treeService.getExchange(session(), desc);
        treeService.releaseExchange(session(), ex1);
        treeService.releaseExchange(session(), ex2);
        assertEquals("cached exchange count", 2, getCachedExchangeCount(ex1.getTree()));
        store().removeTree(session(), desc.getObject());
        assertEquals("cached exchange count", 0, getCachedExchangeCount(ex1.getTree()));
    }

    @Test
    public void maxTreeCache() {
        for(int i = 0; i < (MAX_EXCHANGE_CACHE * 5); ++i) {
            PersistitStorageDescription desc = createDescription("tree_" + i);
            Exchange ex = treeService.getExchange(session(), desc);
            treeService.releaseExchange(session(), ex);
        }
        assertEquals("cached tree count", MAX_TREE_CACHE, treeService.getCachedTreeCount(session()));
    }

    @Test
    public void maxExchangeCache() {
        PersistitStorageDescription desc = createDescription("tree");
        List<Exchange> exchanges = new ArrayList<>();
        for(int i = 0; i < (MAX_EXCHANGE_CACHE * 5); ++i) {
            exchanges.add(treeService.getExchange(session(), desc));
        }
        Tree tree = exchanges.get(0).getTree();
        for(Exchange ex : exchanges) {
            treeService.releaseExchange(session(), ex);
        }
        assertEquals("cached exchange count", MAX_EXCHANGE_CACHE, getCachedExchangeCount(tree));
    }

    @Test
    public void manyTablesTest() {
        for(int i = 0; i < 50; ++i) {
            TableName name = new TableName("test", "t_"+i);
            int tid = createTable(name, "id int not null primary key");
            expectFullRows(tid);
            expectRows(scanAllIndexRequest(getTable(tid).getPrimaryKey().getIndex()));
            ddl().dropTable(session(), name);
        }
        // A little slop for internal trees (e.g. schema manager, index statistics)
        int count = treeService.getCachedTreeCount(session());
        assertTrue("cached tree count: " + count, count < 5);
    }


    private int getCachedExchangeCount(Tree tree) {
        return treeService.exchangeQueue(session(), tree).size();
    }

    private PersistitStorageDescription createDescription(String treeName) {
        PersistitStorageDescription desc = new PersistitStorageDescription(new TestStorage(ais(), "test", treeName), treeName, identifier);
        desc.getObject().setStorageDescription(desc);
        return desc;
    }

    private static class TestStorage extends HasStorage {
        private final AkibanInformationSchema ais;
        private final String schema;
        private final String tree;

        private TestStorage(AkibanInformationSchema ais, String schema, String tree) {
            this.ais = ais;
            this.schema = schema;
            this.tree = tree;
        }

        @Override
        public AkibanInformationSchema getAIS() {
            return ais;
        }

        @Override
        public String getTypeString() {
            return getClass().getSimpleName();
        }

        @Override
        public String getNameString() {
            return tree;
        }

        @Override
        public String getSchemaName() {
            return schema;
        }
    }
}
