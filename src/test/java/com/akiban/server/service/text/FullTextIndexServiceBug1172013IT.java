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
package com.akiban.server.service.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.DuplicateIndexColumnException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionServiceImpl;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types3.mcompat.mfuncs.WaitFunctionHelpers;
import com.akiban.sql.embedded.EmbeddedJDBCService;
import com.akiban.sql.embedded.EmbeddedJDBCServiceImpl;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public class FullTextIndexServiceBug1172013IT extends ITBase {
    public static final String SCHEMA = "test";
    protected FullTextIndexService fullText;
    protected Schema schema;
    protected PersistitAdapter adapter;
    protected QueryContext queryContext;
    private int c;
    private int o;
    private int i;
    private int a;
    private static final Logger logger = LoggerFactory.getLogger(FullTextIndexServiceBug1172013IT.class);
    private Object lock = new Object();
    
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class)
                .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class);
    }

    @Before
    public void createData() {
        c = createTable(SCHEMA, "c",
                        "cid INT PRIMARY KEY NOT NULL",
                        "name VARCHAR(128) COLLATE en_us_ci");
        o = createTable(SCHEMA, "o",
                        "oid INT PRIMARY KEY NOT NULL",
                        "cid INT NOT NULL",
                        "c1  VARCHAR(128) COLLATE en_us_ci",
                        "c2  VARCHAR(128) COLLATE en_us_ci",
                        "c3  VARCHAR(128) COLLATE en_us_ci",
                        "c4  VARCHAR(128) COLLATE en_us_ci",
                        "GROUPING FOREIGN KEY(cid) REFERENCES c(cid)",
                        "order_date DATE");
        i = createTable(SCHEMA, "i",
                        "iid INT PRIMARY KEY NOT NULL",
                        "oid INT NOT NULL",
                        "GROUPING FOREIGN KEY(oid) REFERENCES o(oid)",
                        "sku VARCHAR(10) NOT NULL");
        a = createTable(SCHEMA, "a",
                        "aid INT PRIMARY KEY NOT NULL",
                        "cid INT NOT NULL",
                        "GROUPING FOREIGN KEY(cid) REFERENCES c(cid)",
                        "state CHAR(2)");
        writeRow(c, 1, "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam ultrices justo in sapien ullamcorper eu mattis massa pretium.");
        writeRow(o, 101, 1, "c1", "c2", "c3", "c4", "2012-12-12");
        writeRow(i, 10101, 101, "ABCD");
        writeRow(i, 10102, 101, "1234");
        writeRow(o, 102, 1, "c1", "c2", "c3", "c4","2013-01-01");
        writeRow(a, 101, 1, "MA");
        writeRow(c, 2, "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam ultrices justo in sapien ullamcorper eu mattis massa pretium.");
        writeRow(a, 201, 2, "NY");
        writeRow(c, 3, "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam ultrices justo in sapien ullamcorper eu mattis massa pretium.");
        writeRow(o, 301, 3, "c1", "c2", "c3", "c4", "2010-04-01");
        writeRow(a, 301, 3, "MA");
        writeRow(a, 302, 3, "ME");

        fullText = serviceManager().getServiceByClass(FullTextIndexService.class);

        schema = SchemaCache.globalSchema(ais());
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Test
    public void testDelete1 () throws InterruptedException, PersistitException {
        logger.error("Running test delete 1");
        // This test is specifically for FullTextIndexServiceImpl.java
        assertEquals(FullTextIndexServiceImpl.class, fullText.getClass());
        FullTextIndexServiceImpl fullTextImpl = (FullTextIndexServiceImpl)fullText;

        // disable the populate worker (so it doesn't read all the entries
        // out before we get a chance to look at the tree.
        createFullTextIndex(serviceManager(),
                SCHEMA, "o", "idx3_o",
                "oid", "c1", "c2", "c3", "c4");
        //fullTextImpl.getBackgroundWorks().get(0).forceExecution();
        //WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        
        new Thread(new DropIndex()).start();
        
        try {
            createFullTextIndex(serviceManager(),
                    SCHEMA, "o", "idx3_o",
                    "oid", "c1", "c2", "c3", "c4");
        } catch (DuplicateIndexException ex) {
            logger.error("Got Duplicate Index");
            ; // an expected possible outcome. 
        } catch (DuplicateIndexColumnException ex) {
            logger.error("Got Duplicate Column");
            ; // an expected possible outcome
        }

         WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());

         traverse(fullTextImpl,
                 new Visitor()
                 {
                     int n = 0;
                    
                     @Override
                     public void visit(IndexName idx)
                     {
                         ++n;
                     }

                     @Override
                     public void endOfTree()
                     {
                         assertEquals (0, n);
                     } 
                 });

    }
    
    @Test
    public void testDelete2() throws InterruptedException, PersistitException {
        logger.error("Running test delete 2");
        // This test is specifically for FullTextIndexServiceImpl.java
        assertEquals(FullTextIndexServiceImpl.class, fullText.getClass());
        FullTextIndexServiceImpl fullTextImpl = (FullTextIndexServiceImpl)fullText;

        createFullTextIndex(serviceManager(),
                SCHEMA, "o", "idx3_o",
                "oid", "c1", "c2", "c3", "c4");
        fullTextImpl.getBackgroundWorks().get(0).forceExecution();
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        
        new Thread(new DropIndex()).start();
        
        synchronized (lock) {
            lock.wait();
        }
        
        try {
            createFullTextIndex(serviceManager(),
                    SCHEMA, "o", "idx3_o",
                    "oid", "c1", "c2", "c3", "c4");
        } catch (DuplicateIndexException ex) {
            logger.error("Got Duplicate Index");
            ; // an expected possible outcome. 
        } catch (DuplicateIndexColumnException ex) {
            logger.error("Got Duplicate Column");
            ; // an expected possible outcome
        }

         WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());

         traverse(fullTextImpl,
                 new Visitor()
                 {
                     int n = 0;
                    
                     @Override
                     public void visit(IndexName idx)
                     {
                         ++n;
                     }

                     @Override
                     public void endOfTree()
                     {
                         assertEquals (0, n);
                     } 
                 });


    }
    
    @Test
    public void testDelete3() throws InterruptedException, PersistitException {
        logger.error("Running test delete 3");
        // This test is specifically for FullTextIndexServiceImpl.java
        assertEquals(FullTextIndexServiceImpl.class, fullText.getClass());
        FullTextIndexServiceImpl fullTextImpl = (FullTextIndexServiceImpl)fullText;

        createFullTextIndex(serviceManager(),
                SCHEMA, "o", "idx3_o",
                "oid", "c1", "c2", "c3", "c4");
        fullTextImpl.getBackgroundWorks().get(0).forceExecution();
        //WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        
        deleteFullTextIndex(serviceManager(), new IndexName(new TableName(SCHEMA, "o"), "idx3_o"));
        
         WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());

         traverse(fullTextImpl,
                 new Visitor()
                 {
                     int n = 0;
                    
                     @Override
                     public void visit(IndexName idx)
                     {
                         ++n;
                     }

                     @Override
                     public void endOfTree()
                     {
                         assertEquals (0, n);
                     } 
                 });


    }
    

    @Test
    public void testDropUpdate1 () throws InterruptedException {
        logger.error("Running test drop update 1");
        // This test is specifically for FullTextIndexServiceImpl.java
        assertEquals(FullTextIndexServiceImpl.class, fullText.getClass());
        FullTextIndexServiceImpl fullTextImpl = (FullTextIndexServiceImpl)fullText;

        // create the index, let it complete
        createFullTextIndex(serviceManager(),
                SCHEMA, "o", "idx3_o",
                "oid", "c1", "c2", "c3", "c4");
        fullTextImpl.getBackgroundWorks().get(0).forceExecution();
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        
        writeRow(o, 103, 1, "c1", "c2", "c3", "c4", "2012-12-12");
        writeRow(o, 104, 1, "c1", "c2", "c3", "c4", "2012-12-12");
        writeRow(o, 105, 1, "c1", "c2", "c3", "c4", "2012-12-12");

        new Thread(new DropIndex()).start();
        synchronized (lock) {
            lock.wait();
        }
        
        // kick start the background updater
        fullTextImpl.getBackgroundWorks().get(1).forceExecution();
        // delete the index

    }
    
    private static interface Visitor
    {
        void visit(IndexName idx);
        void endOfTree();
    }

    private class DropIndex implements Runnable
    {
        @Override
        public void run()
        {

            Connection conn;
            try {
                conn = DriverManager.getConnection("jdbc:default:connection", "test", "");
                synchronized (lock) {
                    lock.notify();
                }
                conn.createStatement().execute("DROP INDEX test.o.idx3_o");
            } catch (SQLException e) {
                logger.debug("drop index failed; {}", e.getMessage());
                assertTrue ("Drop index failed", false);
             }
            //IndexName name = new IndexName (new TableName(SCHEMA, "o"), "idx3_o");
            //deleteFullTextIndex(serviceManager(), name);
        }
    }; 

    
    
    private static void traverse(FullTextIndexServiceImpl serv,
            Visitor visitor) throws PersistitException
    {
        Session session = new SessionServiceImpl().createSession();

        try
        {
            Exchange ex = serv.getPopulateExchange(session);
            IndexName toPopulate;
            while ((toPopulate = serv.nextInQueue(session, ex, true)) != null)
            visitor.visit(toPopulate);
            visitor.endOfTree();
        }
        finally
        {
            session.close();
        }
    }
}
