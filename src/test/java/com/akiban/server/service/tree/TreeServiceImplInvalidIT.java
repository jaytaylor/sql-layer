
package com.akiban.server.service.tree;

import com.akiban.server.service.tree.TreeServiceImpl.SchemaNode;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

public class TreeServiceImplInvalidIT extends ITBase {
    @Override
    protected Map<String, String> startupConfigProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("akserver.treespace.a", "drupal*");
        properties.put("akserver.treespace.b", "liveops*");
        return properties;
    }

    @Test
    public void buildInvalidSchemaMaps() throws Exception {
        final TreeServiceImpl treeService = (TreeServiceImpl)serviceManager().getTreeService();
        final SortedMap<String, SchemaNode> result = treeService.getSchemaMap();
        assertEquals(1, result.size());
    }
}
