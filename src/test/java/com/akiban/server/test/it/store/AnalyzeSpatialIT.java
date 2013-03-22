
package com.akiban.server.test.it.store;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.geophile.Space;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collections;

public final class AnalyzeSpatialIT extends ITBase {
    @Test
    public void onlyGeo() {
        int cid = createTable("schem", "tab", "id int not null primary key", "lat decimal(11,07)", "lon decimal(11,7)");

        createSpatialTableIndex("schem", "tab", "idxgeo", 0, Space.LAT_LON_DIMENSIONS, "lat", "lon");
        writeRow(cid, 10L, "10", "11");
        dml().getTableStatistics(session(), cid, false);
        serviceManager().getDXL().ddlFunctions().updateTableStatistics(session(),
                new TableName("schem", "tab"), Collections.singleton("idxgeo"));
        dml().getTableStatistics(session(), cid, false);
    }

    @Test
    public void geoAndOther() {
        int cid = createTable("schem", "tab", "id int not null primary key", "lat decimal(11,07)", "lon decimal(11,7)",
                                              "name varchar(32)");

        createSpatialTableIndex("schem", "tab", "idxgeo", 0, Space.LAT_LON_DIMENSIONS, "lat", "lon", "name");
        writeRow(cid, 10L, "10", "11", "foo");
        dml().getTableStatistics(session(), cid, false);
        serviceManager().getDXL().ddlFunctions().updateTableStatistics(session(),
                new TableName("schem", "tab"), Collections.singleton("idxgeo"));
        dml().getTableStatistics(session(), cid, false);
    }

    @Test
    public void geoGroup() {
        int cid = createTable("schem", "cust", "id int not null primary key", "lat decimal(11,07)", "lon decimal(11,7)",
                "name varchar(32)");
        int oid = createTable("schem", "orders", "id int not null primary key", "cid int not null", "colour varchar(3)",
                akibanFK("cid", "cust", "id"));

        TableName groupName = ais().getUserTable("schem", "cust").getGroup().getName();

        createSpatialGroupIndex(groupName, "idxgeogrp", 0, Space.LAT_LON_DIMENSIONS,
                "cust.lat, cust.lon, orders.colour", Index.JoinType.LEFT);
        writeRow(cid, 10L, "10", "11", "foo");
        writeRow(oid, 20L, 10L, "red");
        dml().getTableStatistics(session(), cid, false);
        serviceManager().getDXL().ddlFunctions().updateTableStatistics(session(),
                new TableName("schem", "orders"), Collections.singleton("idxgeogrp"));
        dml().getTableStatistics(session(), cid, false);
        dml().getTableStatistics(session(), oid, false);
    }
}
