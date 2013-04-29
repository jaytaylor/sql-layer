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

import com.akiban.ais.model.FullTextIndex;
import com.akiban.ais.model.IndexName;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import static com.akiban.qp.operator.API.cursor;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionServiceImpl;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.test.it.qp.TestRow;

import com.akiban.server.types3.mcompat.mfuncs.WaitFunctionHelpers;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.*;

public class FullTextIndexServiceIT extends ITBase
{
    public static final String SCHEMA = "test";
    protected FullTextIndexService fullText;
    protected Schema schema;
    protected PersistitAdapter adapter;
    protected QueryContext queryContext;

    private int c;
    private int o;
    private int i;
    private int a;
    
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Before
    public void createData() {
        c = createTable(SCHEMA, "c",
                        "cid INT PRIMARY KEY NOT NULL",
                        "name VARCHAR(128) COLLATE en_us_ci");
        o = createTable(SCHEMA, "o",
                        "oid INT PRIMARY KEY NOT NULL",
                        "cid INT NOT NULL",
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
        writeRow(c, 1, "Fred Flintstone");
        writeRow(o, 101, 1, "2012-12-12");
        writeRow(i, 10101, 101, "ABCD");
        writeRow(i, 10102, 101, "1234");
        writeRow(o, 102, 1, "2013-01-01");
        writeRow(a, 101, 1, "MA");
        writeRow(c, 2, "Barney Rubble");
        writeRow(a, 201, 2, "NY");
        writeRow(c, 3, "Wilma Flintstone");
        writeRow(o, 301, 3, "2010-04-01");
        writeRow(a, 301, 3, "MA");
        writeRow(a, 302, 3, "ME");

        fullText = serviceManager().getServiceByClass(FullTextIndexService.class);

        schema = SchemaCache.globalSchema(ais());
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }
    
    @Test
    public void testConcurrent_schedule_run_Populate() throws InterruptedException
    {
        // This test is specifically for FullTextIndexServiceImpl.java
        assertEquals(FullTextIndexServiceImpl.class, fullText.getClass());
        FullTextIndexServiceImpl fullTextImpl = (FullTextIndexServiceImpl)fullText;

        // Two threads should run *at the same time* 
        // where
        //      - one tries to add a new entry to the tree
        //      - the other one tries to run populate(), 
        //        which would remove entries from the tree 

        // disable the populate worker (quiet things down!)
        fullTextImpl.disablePopulateWorker();
        
        // create 2 indcies (this should add 2 entries to the tree)
        final FullTextIndex expecteds[] = new FullTextIndex[]
        {
            createFullTextIndex(serviceManager(),
                                SCHEMA, "c", "ft_idx_c", 
                                "name"),
            
            createFullTextIndex(serviceManager(),
                                SCHEMA, "i", "ft_idx_i",
                                "i.sku")
        };
        
        // get a thread to run the populate
        fullTextImpl.runPopulate();
        
        // get this one to create another ft index 
        // (hopefully the two would run at the same time???)
        createFullTextIndex(serviceManager(),
                            SCHEMA, "a", "ft_idx_a",
                            "a.state");
        
       // let the worker do its job.
        fullTextImpl.enablePopulateWorker();
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
    }
        
    @Test
    public void testPopulateScheduling() throws InterruptedException, PersistitException
    {
        // This test is specifically for FullTextIndexServiceImpl.java
        assertEquals(FullTextIndexServiceImpl.class, fullText.getClass());
        FullTextIndexServiceImpl fullTextImpl = (FullTextIndexServiceImpl)fullText;
        
        // disable the populate worker (so it doesn't read all the entries
        // out before we get a chance to look at the tree.
        fullTextImpl.disablePopulateWorker();
        
        // create 3 indices
        final FullTextIndex expecteds[] = new FullTextIndex[]
        {
            createFullTextIndex(serviceManager(),
                                SCHEMA, "c", "idx1_c",
                                "name"),
        
            createFullTextIndex(serviceManager(),
                                SCHEMA, "c", "idx2_c",
                                "i.sku"),
            createFullTextIndex(serviceManager(),
                                SCHEMA, "c", "idx3_c",
                                "name", "i.sku")
        };

        
        // read the entries out
        traverse(fullTextImpl,
                 new Visitor()
                 {
                    int n = 0;
                    public void visit(IndexName idx)
                    {
                        assertEquals("entry[" + n + "]", expecteds[n++].getIndexName(),
                                                         idx);
                    }

                    public void endOfTree()
                    {
                        assertEquals(expecteds.length, n);
                    }
                 });
        

        // let the worker do its job.
        // (After it is done, the tree had better be empty)
        fullTextImpl.enablePopulateWorker();
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
    public void testDeleteIndex() throws PersistitException, InterruptedException
    {
     
        // This test is specifically for FullTextIndexServiceImpl.java
        assertEquals(FullTextIndexServiceImpl.class, fullText.getClass());
        FullTextIndexServiceImpl fullTextImpl = (FullTextIndexServiceImpl)fullText;
         
        // <1> disable worker
        fullTextImpl.disablePopulateWorker();

        // <2> create 3 new indices
         // create 3 indices
        final FullTextIndex expecteds[] = new FullTextIndex[]
        {
            createFullTextIndex(serviceManager(),
                                SCHEMA, "c", "idx4_c",
                                "a.state"),
        
            createFullTextIndex(serviceManager(),
                                SCHEMA, "c", "idx5_c",
                                "i.sku", "a.state"),
            createFullTextIndex(serviceManager(),
                                SCHEMA, "c", "idx6_c",
                                "name", "i.sku")
        };

        // <3> delete 2 of them
        Session session = new SessionServiceImpl().createSession();
        deleteFullTextIndex(serviceManager(), expecteds[0].getIndexName());
        deleteFullTextIndex(serviceManager(), expecteds[1].getIndexName());

        // <4> check that the tree only has one entry now (ie., epxecteds2[2]
        traverse(fullTextImpl,
                 new Visitor()
                 {
                     int n = 0;
                    
                     @Override
                     public void visit(IndexName idx)
                     {
                         assertEquals("entry[" + n++ + "]", expecteds[2].getIndexName(),
                                                            idx);
                     }

                     @Override
                     public void endOfTree()
                     {
                         assertEquals (1, n);
                     } 
                 });
        
        // wake the worker up to do its job
        fullTextImpl.enablePopulateWorker();
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        
        session.close();
    }

    private static interface Visitor
    {
        void visit(IndexName idx);
        void endOfTree();
    }
    
    
    private static void traverse(FullTextIndexServiceImpl serv,
                                 Visitor visitor) throws PersistitException
    {
         Session session = new SessionServiceImpl().createSession();

         try
         {
             Exchange ex = serv.getPopulateExchange(session);
             IndexName toPopulate;
             while ((toPopulate = serv.nextInQueue(ex)) != null)
                 visitor.visit(toPopulate);
             visitor.endOfTree();
         }
         finally
         {
             session.close();
         }
    }

    @Test
    public void testUpdate() throws InterruptedException
    {
        
        /*
            Test plans:
            1 - do a search, confirm that the rows come back as expected
            2 - (With update worker enabled)
                + insert new rows
                + wait for the updates to be done
                + do the search again and confirm that the new rows are found
            3 - (With update worker NOT enable)
                + disable update worker
                + insert new rows
                + do the search again and confirm that the new rows are NOT found

         */

        //CREATE INDEX cust_ft ON customers(FULL_TEXT(name, addresses.state, items.sku))

        // part 1
        FullTextIndex index = createFullTextIndex(serviceManager(),
                                                  SCHEMA, "c", "idx_c", 
                                                  "name", "i.sku", "a.state");

        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        RowType rowType = rowType("c");
        RowBase[] expected1 = new RowBase[]
        {
            row(rowType, 1L),
            row(rowType, 3L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        Operator plan = builder.scanOperator("flintstone", 15);
        compareRows(expected1, cursor(plan, queryContext));

        plan = builder.scanOperator("state:MA", 15);
        compareRows(expected1, cursor(plan, queryContext));
        
        
        // part 2
        // write new rows
        writeRow(c, 4, "John Watson");
        writeRow(c, 5, "Sherlock Flintstone");
        writeRow(c, 6, "Mycroft Holmes");
        writeRow(c, 7, "Flintstone Lestrade");
        
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        RowBase expected2[] = new RowBase[]
        {
            row(rowType, 1L),
            row(rowType, 3L),
            row(rowType, 5L),
            row(rowType, 7L)
        };

        // confirm new changes
        plan = builder.scanOperator("flintstone", 15);
        compareRows(expected2, cursor(plan, queryContext));
        
        // part 3
        ((FullTextIndexServiceImpl)fullText).disableUpdateWorker();
        
        writeRow(c, 8, "Flintstone Hudson");
        writeRow(c, 9, "Jim Flintstone");
        
        // The worker has been disabled, waitOn should return immidately
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        
        // confirm that new rows are not found (ie., expected2 still works)
        plan = builder.scanOperator("flintstone", 15);
        compareRows(expected2, cursor(plan, queryContext));
        
        ((FullTextIndexServiceImpl)fullText).enableUpdateWorker();
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        
        // now the rows should be seen.
        // (Because disabling the worker does not stop the changes fron being recorded)
        RowBase expected3[] = new RowBase[]
        {
            row(rowType, 1L),
            row(rowType, 3L),
            row(rowType, 5L),
            row(rowType, 7L),
            row(rowType, 8L),
            row(rowType, 9L)
        };

        plan = builder.scanOperator("flintstone", 15);
        compareRows(expected3, cursor(plan, queryContext));
    }

    @Test
    public void cDown() throws InterruptedException {
        FullTextIndex index = createFullTextIndex(serviceManager(),
                                                  SCHEMA, "c", "idx_c", 
                                                  "name", "i.sku", "a.state");
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        RowType rowType = rowType("c");
        RowBase[] expected = new RowBase[] {
            row(rowType, 1L),
            row(rowType, 3L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        Operator plan = builder.scanOperator("flintstone", 10);
        compareRows(expected, cursor(plan, queryContext));

        plan = builder.scanOperator("state:MA", 10);
        compareRows(expected, cursor(plan, queryContext));
        
    }

    @Test
    public void oUpDown() throws InterruptedException {
        FullTextIndex index = createFullTextIndex(serviceManager(),
                                                  SCHEMA, "o", "idx_o",
                                                  "c.name", "i.sku");
        WaitFunctionHelpers.waitOn(fullText.getBackgroundWorks());
        RowType rowType = rowType("o");
        RowBase[] expected = new RowBase[] {
            row(rowType, 1L, 101L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        Operator plan = builder.scanOperator("name:Flintstone AND sku:1234", 10);
        compareRows(expected, cursor(plan, queryContext));
    }

    protected RowType rowType(String tableName) {
        return schema.newHKeyRowType(ais().getUserTable(SCHEMA, tableName).hKey());
    }

    protected TestRow row(RowType rowType, Object... fields) {
        return new TestRow(rowType, fields);
    }

}
