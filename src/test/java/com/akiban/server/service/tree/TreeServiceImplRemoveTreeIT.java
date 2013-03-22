
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
