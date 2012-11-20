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

package com.akiban.server.test.it.store;

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
}
