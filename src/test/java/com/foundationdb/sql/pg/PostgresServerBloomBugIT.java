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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This test was created due to the multiple possible plans that were being created as the loader input of a Bloom Filter
 * Previously this was all done under the assumption of an indexScan being the only possible loader
 * For this specific instance a Project it placed on top of the index scan
 * Because of this we now depend on the rowtype of the input stream to create the collators
 *
 */
public class PostgresServerBloomBugIT extends PostgresServerITBase{

    String DirectoryLocation = "src/test/resources/com/foundationdb/sql/optimizer/operator/coi-index/";
    String StatsFile = DirectoryLocation + "stats.yaml";
    String sql = "SELECT items.sku FROM items, categories WHERE items.sku = categories.sku AND categories.cat = 1 ORDER BY items.sku";
    Connection connection;



    String schemaSetup[] = {
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

    protected int rootTableId;

    public void loadDatabase(File dir) throws Exception {
        this.rootTableId = super.loadDatabase(SCHEMA_NAME, dir);
    }
    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> map = new HashMap<>(super.startupConfigProperties());
        map.put("optimizer.scaleIndexStatistics", "false");
        return map;
    }

    @Before
    public void setup() throws Exception{
        connection = getConnection();
        for(String sqlStatement : schemaSetup) {
            connection.createStatement().execute(sqlStatement);
        }
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                File file = new File(StatsFile);
                if (file.exists()) {
                    IndexStatisticsService service = serviceManager().getServiceByClass(IndexStatisticsService.class);
                    service.loadIndexStatistics(session(), SCHEMA_NAME, file);
                }
                return null;
            }
        });
    }


    @Test
    public void test() throws Exception {
        connection.createStatement().execute(sql);

    }
}
