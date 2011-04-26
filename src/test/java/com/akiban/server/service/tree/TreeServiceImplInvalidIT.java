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

import com.akiban.server.service.config.Property;
import com.akiban.server.service.tree.TreeServiceImpl.SchemaNode;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

public class TreeServiceImplInvalidIT extends ITBase {
    @Override
    protected Collection<Property> startupConfigProperties() {
        final Collection<Property> properties = new ArrayList<Property>();
        properties.add(new Property("akserver.treespace.a", "drupal*"));
        properties.add(new Property("akserver.treespace.b", "liveops*"));
        return properties;
    }

    @Test
    public void buildInvalidSchemaMaps() throws Exception {
        final TreeServiceImpl treeService = (TreeServiceImpl)serviceManager().getTreeService();
        final SortedMap<String, SchemaNode> result = treeService.getSchemaMap();
        assertEquals(1, result.size());
    }
}
