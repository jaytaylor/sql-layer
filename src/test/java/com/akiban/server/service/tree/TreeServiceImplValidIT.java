/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.service.tree;

import com.akiban.server.service.tree.TreeServiceImpl.SchemaNode;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TreeServiceImplValidIT extends ITBase {
    @Override
    protected Map<String, String> startupConfigProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("akserver.treespace.a",
                                    "drupal*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G");
        properties.put("akserver.treespace.b",
                                    "liveops*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G");
        properties.put("akserver.treespace.c",
                                    "test*/_schema_:${datapath}/${schema}${tree}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G");
        return properties;
    }

    @Test
    public void buildValidSchemaMap() throws Exception {
        final TreeServiceImpl treeService = (TreeServiceImpl) serviceManager().getTreeService();
        final SortedMap<String, SchemaNode> result = treeService.getSchemaMap();
        assertEquals(4, result.size()); // +1 for default in base properties
        final String vs1 = treeService.volumeForTree("drupalxx","_schema_");
        final String vs2 = treeService.volumeForTree("liveops","_schema_");
        final String vs3 = treeService.volumeForTree("tpcc", "_schema_");
        final String vs4 = treeService.volumeForTree("test42", "_schema_");
        assertTrue(vs1.contains("drupalxx.v0"));
        assertTrue(vs2.contains("liveops.v0"));
        assertTrue(vs3.contains("akiban_data"));
        assertTrue(vs4.contains("test42_schema_.v0"));
    }
}
