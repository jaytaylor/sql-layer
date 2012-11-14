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
        final Map<String, String> properties = new HashMap<String, String>();
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
