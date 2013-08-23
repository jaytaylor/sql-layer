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

import com.foundationdb.server.service.tree.TreeServiceImpl.SchemaNode;
import com.foundationdb.server.test.it.PersistitITBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

public class TreeServiceImplInvalidIT extends PersistitITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("fdbsql.treespace.a", "drupal*");
        properties.put("fdbsql.treespace.b", "liveops*");
        return properties;
    }

    @Test
    public void buildInvalidSchemaMaps() throws Exception {
        final TreeServiceImpl treeService = (TreeServiceImpl)treeService();
        final SortedMap<String, SchemaNode> result = treeService.getSchemaMap();
        assertEquals(1, result.size());
    }
}
