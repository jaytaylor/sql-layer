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

package com.foundationdb.server.test.it.store;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collections;

public final class AnalyzeSpatialIT extends ITBase {
    @Test
    public void onlyGeo() {
        int cid = createTable("schem", "tab", "id int not null primary key", "lat decimal(11,07)", "lon decimal(11,7)");

        createSpatialTableIndex("schem", "tab", "idxgeo", 0, 2, "lat", "lon");
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

        createSpatialTableIndex("schem", "tab", "idxgeo", 0, 2, "lat", "lon", "name");
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

        TableName groupName = ais().getTable("schem", "cust").getGroup().getName();

        createSpatialGroupIndex(groupName, "idxgeogrp", 0, Spatial.LAT_LON_DIMENSIONS, Index.JoinType.LEFT,
                                "cust.lat", "cust.lon", "orders.colour");
        writeRow(cid, 10L, "10", "11", "foo");
        writeRow(oid, 20L, 10L, "red");
        dml().getTableStatistics(session(), cid, false);
        serviceManager().getDXL().ddlFunctions().updateTableStatistics(session(),
                new TableName("schem", "orders"), Collections.singleton("idxgeogrp"));
        dml().getTableStatistics(session(), cid, false);
        dml().getTableStatistics(session(), oid, false);
    }
}
