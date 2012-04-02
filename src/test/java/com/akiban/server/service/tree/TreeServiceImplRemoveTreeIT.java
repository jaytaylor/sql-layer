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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.test.it.ITBase;
import com.persistit.Exchange;
import com.persistit.Tree;

public class TreeServiceImplRemoveTreeIT extends ITBase {
 
    
    @Test
    public void verifyDeleteTreeReleasesResources() throws Exception {
        final TreeServiceImpl treeService = (TreeServiceImpl) serviceManager().getTreeService();
        final SessionService sessionService = (SessionService) serviceManager().getSessionService();
        final Session session = sessionService.createSession();
        final Exchange ex1 = treeService.getExchange(session, new TestLink("schema", "someTree"));
        final Tree tree = ex1.getTree();
        treeService.releaseExchange(session, ex1);
        assertFalse(treeService.exchangeList(session, ex1.getTree()).isEmpty());
        final Exchange ex2 = treeService.getExchange(session, new TestLink("schema", "someTree"));
        final Exchange ex3 = treeService.getExchange(session, new TestLink("schema", "someTree"));
        treeService.releaseExchange(session, ex3);
        ex2.removeTree();
        treeService.releaseExchange(session, ex2);
        assertTrue(treeService.exchangeList(session, tree).isEmpty());
    }

}
