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

package com.foundationdb.sql.pg;

import com.foundationdb.server.store.statistics.IndexStatisticsService;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This test was created due to the multiple possible plans that were being created as the loader input of a Bloom Filter
 * Previously this was all done under the assumption of an indexScan being the only possible loader
 * Because of this we now depend on the rowtype of the input stream to create the collator
 *
 * In order to guarantee the use of the Bloom Filter a stats.yaml file is loaded, the same used in operator/coi-index
 * Also the optimizer.scaleIndexStatistics property has to be set to false in order to allow these stats to have an effect
 * on the perceived row counts on each side of the join
 */

public class PostgresServerBloomFilterIT extends PostgresServerFilesITBase
{

    private static final String RESOURCE_LOCATION = "com/foundationdb/sql/optimizer/operator/coi-index/";
    private static final String STATS_FILE = RESOURCE_LOCATION + "stats.yaml";
    private static final String SQL = "SELECT items.sku FROM items, categories WHERE items.sku = categories.sku AND categories.cat = 1 ORDER BY items.sku";
    Connection connection;

    private static final String SCHEMA_SETUP[] = {
    "CREATE TABLE customers(cid int NOT NULL,PRIMARY KEY(cid), name varchar(32) NOT NULL);",
    "CREATE INDEX name ON customers(name);",
    "CREATE TABLE orders(oid int NOT NULL,PRIMARY KEY(oid),cid int NOT NULL,order_date date NOT NULL, GROUPING FOREIGN KEY (cid) REFERENCES customers(cid));",
    "CREATE INDEX \"__akiban_fk_0\" ON orders(cid);",
    "CREATE INDEX order_date ON orders(order_date);",
    "CREATE TABLE items(iid int NOT NULL,PRIMARY KEY(iid),oid int NOT NULL, sku varchar(32) NOT NULL, quan int NOT NULL,GROUPING FOREIGN KEY (oid) REFERENCES orders(oid));",
    "CREATE INDEX sku ON items(sku);",
    "CREATE TABLE handling (hid int NOT NULL, PRIMARY KEY(hid), iid int NOT NULL, GROUPING FOREIGN KEY (iid) REFERENCES items(iid));",
    "CREATE TABLE categories(cat int NOT NULL, sku varchar(32) NOT NULL);",
    "CREATE UNIQUE INDEX cat_sku ON categories(cat,sku);",
    "INSERT INTO items values(1,2,\'car\', 10),(3,4,\'chair\',11),(4,5,\'desk\',12);",
    "INSERT INTO categories values(1,\'bug\'),(1,\'chair\'),(2,\'desk\'),(15,\'nothing\');"
    };

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> map = new HashMap<>(super.startupConfigProperties());
        map.put("optimizer.scaleIndexStatistics", "false");
        return map;
    }

    @Before
    public void setup() throws Exception {
        connection = getConnection();
        for(String sqlStatement : SCHEMA_SETUP) {
            connection.createStatement().execute(sqlStatement);
        }
        final File file = copyResourceToTempFile("/" + STATS_FILE);
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                IndexStatisticsService service = serviceManager().getServiceByClass(IndexStatisticsService.class);
                service.loadIndexStatistics(session(), SCHEMA_NAME, file);
                return null;
            }
        });
    }

    @Test
    public void test() throws Exception {
        Statement statement  = connection.createStatement();
        ResultSet planRS = statement.executeQuery("EXPLAIN " + SQL);
        boolean found = false;
        while (!found && planRS.next()) {
            String row = (String)planRS.getObject(1);
            if (row.contains("Using_BloomFilter"))
                found = true;
        }
        assert(found);
        ResultSet outputRS = statement.executeQuery( SQL);
        outputRS.next();
        assert(outputRS.getObject(1).equals("chair"));
        assert(!outputRS.next());
    }
}
