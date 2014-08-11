package com.foundationdb.sql.pg;

import com.foundationdb.server.store.statistics.IndexStatisticsService;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.util.concurrent.Callable;

/**
 * Created by jerett on 8/11/14.
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
    "CREATE UNIQUE INDEX cat_sku ON categories(cat,sku);"};

    protected int rootTableId;

    public void loadDatabase(File dir) throws Exception {
        this.rootTableId = super.loadDatabase(SCHEMA_NAME, dir);
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
