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
package com.foundationdb.server.service.text;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.DuplicateIndexException;
import com.foundationdb.server.service.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Originally testing various sequences of background population but population
 * is now done in the foreground.
 */
public class FullTextIndexServiceBug1172013IT extends FullTextIndexServiceITBase {
    private static final Logger logger = LoggerFactory.getLogger(FullTextIndexServiceBug1172013IT.class);
    private final Object lock = new Object();

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

        schema = SchemaCache.globalSchema(ais());
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
    }

    /** Race concurrent drop vs create. */
    @Test
    public void testDelete1 () throws InterruptedException {
        logger.debug("Running test delete 1");

        createFullTextIndex(
                SCHEMA, "o", "idx3_o",
                "oid", "c1", "c2", "c3", "c4");

        Thread t = new Thread(new DropIndex());
        t.start();

        try {
            createFullTextIndex(
                    SCHEMA, "o", "idx3_o",
                    "oid", "c1", "c2", "c3", "c4");
        } catch(DuplicateIndexException e) {
            // Possible outcome
        }

        t.join();

        if(getTable(SCHEMA, "o").getFullTextIndex("idx3_o") != null) {
            new DropIndex().run();
        }
    }

    /** Race concurrent drop vs create, with lined up start. */
    @Test
    public void testDelete2() throws InterruptedException {
        logger.debug("Running test delete 2");
        createFullTextIndex(
                SCHEMA, "o", "idx3_o",
                "oid", "c1", "c2", "c3", "c4");

        Thread t = new Thread(new DropIndex());
        t.start();

        synchronized (lock) {
            lock.wait();
        }
        
        try {
            createFullTextIndex(
                    SCHEMA, "o", "idx3_o",
                    "oid", "c1", "c2", "c3", "c4");
        } catch (DuplicateIndexException ex) {
            logger.debug("Got Duplicate Index");
            // an expected possible outcome.
        }

        t.join();

        if(getTable(SCHEMA, "o").getFullTextIndex("idx3_o") != null) {
            new DropIndex().run();
        }
    }

    /** Serial create and drop. */
    @Test
    public void testDelete3() throws InterruptedException {
        logger.debug("Running test delete 3");
        createFullTextIndex(
                SCHEMA, "o", "idx3_o",
                "oid", "c1", "c2", "c3", "c4");

        new DropIndex().run();
    }

    @Test
    public void testDropUpdate1 () throws InterruptedException {
        logger.debug("Running test drop update 1");

        // create the index, let it complete
        createFullTextIndex(
                SCHEMA, "o", "idx3_o",
                "oid", "c1", "c2", "c3", "c4");

        writeRow(o, 103, 1, "c1", "c2", "c3", "c4", "2012-12-12");
        writeRow(o, 104, 1, "c1", "c2", "c3", "c4", "2012-12-12");
        writeRow(o, 105, 1, "c1", "c2", "c3", "c4", "2012-12-12");

        // Race update vs drop
        Thread t = new Thread(new DropIndex());
        t.start();
        // But don't let it fall off the end
        t.join();
    }

    private class DropIndex implements Runnable
    {
        @Override
        public void run()
        {
            Session session = serviceManager().getSessionService().createSession();
            TableName tableName = new TableName (SCHEMA, "o");
            synchronized (lock) {
                lock.notify();
            }
            ddl().dropTableIndexes(session, tableName, Collections.singletonList("idx3_o"));
        }
    }
}
