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

import com.akiban.server.test.it.ITBase;
import com.persistit.Exchange;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TreeServiceImplVolumeIT extends ITBase {

    @Override
    protected Map<String, String> startupConfigProperties() {
        final Map<String, String> properties = new HashMap<String, String>();
        properties.put("akserver.treespace.a",
                                    "drupal*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G");
        properties.put("akserver.treespace.b",
                                    "liveops*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G");
        return properties;
    }

    @Test
    public void testCreateVolume() throws Exception {
        final TreeService treeService = serviceManager().getTreeService();
        final TestLink link0 = new TestLink("not_drupal", "_schema_");
        checkExchangeName(treeService, link0, "akiban_data");
        final TestLink link1 = new TestLink("drupal_large", "_schema_");
        checkExchangeName(treeService, link1, "drupal_large");
        final TestLink link2 = new TestLink("drupal.org", "_schema_");
        checkExchangeName(treeService, link2, "drupal.org");
        final Set<Tree> trees = new HashSet<Tree>();
        treeService.visitStorage(session(), new TreeVisitor() {
            @Override
            public void visit(Exchange exchange) throws PersistitException {
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

    private int verifyTableId(final TreeService treeService, final int aisId, TreeLink link)
            throws PersistitException {
        final int stored = treeService.aisToStore(link, aisId);
        final int recovered = treeService.storeToAis(link, stored);
        assertEquals(recovered, aisId);
        return aisId - stored;
    }
}
