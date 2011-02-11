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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import org.junit.Test;

import com.akiban.server.CServerTestCase;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.server.service.tree.TreeServiceImpl.SchemaNode;
import com.persistit.Exchange;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;

public class TreeServiceImplTest extends CServerTestCase {

    private class TestLink implements TreeLink {
        final String schemaName;
        final String treeName;
        TreeCache cache;

        TestLink(String s, String t) {
            schemaName = s;
            treeName = t;
        }

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
    }

    @Test
    public void buildValidSchemaMap() throws Exception {
        final Collection<Property> properties = new ArrayList<Property>();
        properties.add(property("cserver", "treespace.a",
                "drupal*:${datapath}/${schema}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        properties.add(property("cserver", "treespace.b",
                "liveops*:${datapath}/${schema}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        properties.add(property("cserver", "treespace.c",
                "test*/_schema_:${datapath}/${schema}${tree}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        baseSetUp(properties);
        try {
            final TreeServiceImpl treeService = (TreeServiceImpl) ServiceManagerImpl
                    .get().getTreeService();
            final SortedMap<String, SchemaNode> result = treeService
                    .getSchemaMap();
            assertEquals(4, result.size()); // +1 for default in base properties
            final String vs1 = treeService.volumeForTree("drupalxx",
                    "_schema_");
            final String vs2 = treeService.volumeForTree("liveops",
                    "_schema_");
            final String vs3 = treeService.volumeForTree("tpcc", "_schema_");
            final String vs4 = treeService.volumeForTree("test42", "_schema_");
            assertTrue(vs1.contains("drupalxx.v0"));
            assertTrue(vs2.contains("liveops.v0"));
            assertTrue(vs3.contains("akiban_data"));
            assertTrue(vs4.contains("test42_schema_.v0"));
        } finally {
            baseTearDown();
        }
    }

    @Test
    public void buildInvalidSchemaMaps() throws Exception {
        final Collection<Property> properties = new ArrayList<Property>();
        properties.add(property("cserver", "treespace.a", "drupal*"));
        properties.add(property("cserver", "treespace.b", "liveops*"));
        baseSetUp(properties);
        try {
            final TreeServiceImpl treeService = (TreeServiceImpl) ServiceManagerImpl
                    .get().getTreeService();
            final SortedMap<String, SchemaNode> result = treeService
                    .getSchemaMap();
            assertEquals(1, result.size());
        } finally {
            baseTearDown();
        }
    }

    @Test
    public void testCreateVolume() throws Exception {
        final Collection<Property> properties = new ArrayList<Property>();
        properties.add(property("cserver", "treespace.a",
                "drupal*:${datapath}/${schema}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        properties.add(property("cserver", "treespace.b",
                "liveops*:${datapath}/${schema}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        baseSetUp(properties);
        try {
            final TreeService treeService = ServiceManagerImpl.get()
                    .getTreeService();
            final Session session = new SessionImpl();
            final TestLink link0 = new TestLink("not_drupal", "_schema_");
            final Exchange ex0 = treeService.getExchange(session, link0);
            assertEquals("akiban_data", ex0.getVolume().getName());
            final TestLink link1 = new TestLink("drupal_large", "_schema_");
            final Exchange ex1 = treeService.getExchange(session, link1);
            assertEquals("drupal_large", ex1.getVolume().getName());
            final TestLink link2 = new TestLink("drupal.org", "_schema_");
            final Exchange ex2 = treeService.getExchange(session, link2);
            assertEquals("drupal.org", ex2.getVolume().getName());
            final Set<Tree> trees = new HashSet<Tree>();
            treeService.visitStorage(session, new TreeVisitor() {
                @Override
                public void visit(Exchange exchange) throws Exception {
                    trees.add(exchange.getTree());
                }
            }, "_schema_");
            assertEquals(3, trees.size());
            final int d0 = verifyTableId(treeService, 1, link0);
            final int d1 = verifyTableId(treeService, 100002, link1);
            final int d2 = verifyTableId(treeService, 200003, link2);
            assertEquals(0, d0);
            assertTrue(d1 > d0);
            assertTrue(d2 > d1);
        } finally {
            baseTearDown();
        }

    }

    private int verifyTableId(final TreeService treeService, final int aisId,
            TreeLink link) throws PersistitException {
        final int stored = treeService.aisToStore(link, aisId);
        final int recovered = treeService.storeToAis(link, stored);
        assertEquals(recovered, aisId);
        return aisId - stored;
    }
}
