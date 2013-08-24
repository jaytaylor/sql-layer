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

import com.foundationdb.server.test.it.PersistitITBase;
import com.persistit.Exchange;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TreeServiceImplVolumeIT extends PersistitITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("fdbsql.treespace.a",
                                    "drupal*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G");
        properties.put("fdbsql.treespace.b",
                                    "liveops*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G");
        return properties;
    }

    @Test
    public void testCreateVolume() throws Exception {
        final TreeService treeService = treeService();
        final TestLink link0 = new TestLink("not_drupal", "_schema_");
        checkExchangeName(treeService, link0, "persistit_data");
        final TestLink link1 = new TestLink("drupal_large", "_schema_");
        checkExchangeName(treeService, link1, "drupal_large");
        final TestLink link2 = new TestLink("drupal.org", "_schema_");
        checkExchangeName(treeService, link2, "drupal.org");
        final Set<Tree> trees = new HashSet<>();
        treeService.visitStorage(session(), new TreeVisitor() {
            @Override
            public void visit(Exchange exchange) throws PersistitException {
                trees.add(exchange.getTree());
            }
        }, "_schema_");
        assertEquals(3, trees.size());
    }

    private void checkExchangeName(TreeService treeService, TestLink link, String expectedName) throws
    PersistitException {
        final Exchange exchange = treeService.getExchange(session(), link);
        try {
            assertEquals(expectedName, exchange.getVolume().getName());
        } finally {
            treeService.releaseExchange(session(), exchange);
        }
    }
}
