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

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.geophile.SpaceLatLon;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpatialLatLonIndexScanIT extends OperatorITBase
{
    @Before
    public void before()
    {
        point = createTable(
            "schema", "point",
            "id int not null",
            "lat decimal(11, 7)",
            "lon decimal(11, 7)",
            "primary key(id)");
        TableIndex latLonIndex = createIndex("schema", "point", "latlon", "lat", "lon");
        latLonIndex.setIndexMethod(Index.IndexMethod.Z_ORDER_LAT_LON);
        schema = new Schema(rowDefCache().ais());
        pointRowType = schema.userTableRowType(userTable(point));
        latLonIndexRowType = indexType(point, "lat", "lon");
        space = SpaceLatLon.create();
        db = new NewRow[]{
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
    }

    @Test
    public void testLoad()
    {
        loadDB();
        {
            // Check index
            Operator plan = indexScan_Default(latLonIndexRowType);
            RowBase[] expected = new RowBase[zToId.size()];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = row(latLonIndexRowType, z, (long) id);
            }
            compareRows(expected, cursor(plan, queryContext));
        }
    }

    @Test
    public void testLoadAndRemove()
    {
        loadDB();
        {
            // Delete rows with odd ids
            for (Integer id : zToId.values()) {
                if ((id % 2) == 1) {
                    dml().deleteRow(session(), createNewRow(point, id, lats.get(id), lons.get(id)));
                }
            }
        }
        {
            // Check index
            Operator plan = indexScan_Default(latLonIndexRowType);
            int rowsRemaining = zToId.size() / 2;
            if ((zToId.size() % 2) == 1) {
                rowsRemaining += 1;
            }
            RowBase[] expected = new RowBase[rowsRemaining];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                // Only even ids should remain
                if ((id % 2) == 0) {
                    expected[r++] = row(latLonIndexRowType, z, (long) id);
                }
            }
            compareRows(expected, cursor(plan, queryContext));
        }
    }

    @Test
    public void testLoadAndUpdate()
    {
        loadDB();
        int n = lats.size();
        zToId.clear();
        {
            // Increment y values
            for (int id = 0; id < n; id++) {
                BigDecimal lat = lats.get(id);
                BigDecimal lon = lons.get(id);
                NewRow before = createNewRow(point, id, lat, lon);
                NewRow after = createNewRow(point, id, lat, lon.add(BigDecimal.ONE));
                long z = space.shuffle(new BigDecimal[]{lat, lon.add(BigDecimal.ONE)});
                zToId.put(z, id);
                dml().updateRow(session(), before, after, null);
            }
        }
        {
            // Check index
            Operator plan = indexScan_Default(latLonIndexRowType);
            RowBase[] expected = new RowBase[zToId.size()];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = row(latLonIndexRowType, z, (long) id);
            }
            compareRows(expected, cursor(plan, queryContext));
        }
    }

    @Test
    public void testSpatialQuery()
    {
        loadDB();
        final int N = 100;
        BigDecimal latLo;
        BigDecimal latHi;
        BigDecimal lonLo;
        BigDecimal lonHi;
        for (int i = 0; i < N; i++) {
            latLo = randomLat();
            latHi = randomLat();
            if (latLo.compareTo(latHi) > 0) {
                BigDecimal swap = latLo;
                latLo = latHi;
                latHi = swap;
            }
            lonLo = randomLon();
            lonHi = randomLon();
            if (lonLo.compareTo(lonHi) > 0) {
                BigDecimal swap = lonLo;
                lonLo = lonHi;
                lonHi = swap;
            }
            // Get the right answer
            Set<Integer> expected = new HashSet<Integer>();
            for (int id = 0; id < lats.size(); id++) {
                BigDecimal lat = lats.get(id);
                BigDecimal lon = lons.get(id);
                if (latLo.compareTo(lat) <= 0 &&
                    lat.compareTo(latHi) <= 0 &&
                    lonLo.compareTo(lon) <= 0 &&
                    lon.compareTo(lonHi) <= 0) {
                    expected.add(id);
                }
            }
            // Get the query result
            Set<Integer> actual = new HashSet<Integer>();
            IndexBound lowerLeft = new IndexBound(row(latLonIndexRowType, latLo, lonLo),
                                                  new SetColumnSelector(0, 1));
            IndexBound upperRight = new IndexBound(row(latLonIndexRowType, latHi, lonHi),
                                                   new SetColumnSelector(0, 1));
            IndexKeyRange box = IndexKeyRange.spatial(latLonIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(latLonIndexRowType, false, box);
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
            Row row;
            while ((row = cursor.next()) != null) {
                int id = getLong(row, 1).intValue();
                actual.add(id);
            }
            // There should be no false negatives
            assertTrue(actual.containsAll(expected));
        }
    }

    @Test
    public void testSpatialQueryWithWraparound()
    {
        loadDB();
        final int N = 100;
        BigDecimal latLo;
        BigDecimal latHi;
        BigDecimal lonLo;
        BigDecimal lonHi;
        for (int i = 0; i < N; i++) {
            latLo = randomLat();
            latHi = randomLat();
            if (latLo.compareTo(latHi) > 0) {
                BigDecimal swap = latLo;
                latLo = latHi;
                latHi = swap;
            }
            lonLo = randomLon();
            lonHi = randomLon();
            if (lonLo.compareTo(lonHi) < 0) {
                // Guarantee wraparound
                BigDecimal swap = lonLo;
                lonLo = lonHi;
                lonHi = swap;
            }
            // Get the right answer
            Set<Integer> expected = new HashSet<Integer>();
            for (int id = 0; id < lats.size(); id++) {
                BigDecimal lat = lats.get(id);
                BigDecimal lon = lons.get(id);
                if (latLo.compareTo(lat) <= 0 &&
                    lat.compareTo(latHi) <= 0 &&
                    lonLo.compareTo(lon) <= 0 &&
                    lon.compareTo(lonHi) <= 0) {
                    expected.add(id);
                }
            }
            // Get the query result
            Set<Integer> actual = new HashSet<Integer>();
            IndexBound lowerLeft = new IndexBound(row(latLonIndexRowType, latLo, lonLo),
                                                  new SetColumnSelector(0, 1));
            IndexBound upperRight = new IndexBound(row(latLonIndexRowType, latHi, lonHi),
                                                   new SetColumnSelector(0, 1));
            IndexKeyRange box = IndexKeyRange.spatial(latLonIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(latLonIndexRowType, false, box);
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
            Row row;
            while ((row = cursor.next()) != null) {
                int id = getLong(row, 1).intValue();
                actual.add(id);
            }
            // There should be no false negatives
            assertTrue(actual.containsAll(expected));
        }
    }

    @Test
    public void testNearPoint()
    {
        loadDB();
        final int N = 100;
        for (int i = 0; i < N; i++) {
            BigDecimal queryLat = randomLat();
            BigDecimal queryLon = randomLon();
            long zStart = space.shuffle(queryLat, queryLon);
            IndexBound zStartBound = new IndexBound(row(latLonIndexRowType.physicalRowType(), queryLat, queryLon),
                                                    new SetColumnSelector(0, 1));
            IndexKeyRange zStartRange = IndexKeyRange.around(latLonIndexRowType, zStartBound);
            Operator plan = indexScan_Default(latLonIndexRowType, false, zStartRange);
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
            Row row;
            long previousDistance = Long.MIN_VALUE;
            int count = 0;
            while ((row = cursor.next()) != null) {
                long zActual = row.eval(0).getLong();
                int id = getLong(row, 1).intValue();
                BigDecimal lat = lats.get(id);
                BigDecimal lon = lons.get(id);
                long zExpected = space.shuffle(lat, lon);
                assertEquals(zExpected, zActual);
                long distance = abs(zExpected - zStart);
                assertTrue(distance >= previousDistance);
                previousDistance = distance;
                count++;
            }
            assertEquals(zToId.size(), count);
        }
    }

    private void loadDB()
    {
        int id = 0;
        for (long y = LAT_LO; y <= LAT_HI; y += DLAT) {
            for (long x = LON_LO; x < LON_HI; x += DLON) {
                BigDecimal lat = new BigDecimal(y);
                BigDecimal lon = new BigDecimal(x);
                dml().writeRow(session(), createNewRow(point, id, lat, lon));
                long z = space.shuffle(SpaceLatLon.scaleLat(lat), SpaceLatLon.scaleLon(lon));
                zToId.put(z, id);
                lats.add(lat);
                lons.add(lon);
                zs.add(z);
                id++;
            }
        }
    }

    private BigDecimal randomLat()
    {
        return new BigDecimal(random.nextDouble() * LAT_RANGE + LAT_LO);
    }

    private BigDecimal randomLon()
    {
        return new BigDecimal(random.nextDouble() * LON_RANGE + LON_LO);
    }

    private static final int LAT_LO = -90;
    private static final int LAT_HI = 90;
    private static final int LON_LO = -180;
    private static final int LON_HI = 180;
    private static final int LAT_RANGE = LAT_HI - LAT_LO;
    private static final int LON_RANGE = LON_HI - LON_LO;
    private static final int DLAT = 10;
    private static final int DLON = 10;

    private int point;
    private UserTableRowType pointRowType;
    private IndexRowType latLonIndexRowType;
    private SpaceLatLon space;
    private Map<Long, Integer> zToId = new TreeMap<Long, Integer>();
    List<BigDecimal> lats = new ArrayList<BigDecimal>(); // indexed by id
    List<BigDecimal> lons = new ArrayList<BigDecimal>(); // indexed by id
    List<Long> zs = new ArrayList<Long>(); // indexed by id
    Random random = new Random(123456);
}
