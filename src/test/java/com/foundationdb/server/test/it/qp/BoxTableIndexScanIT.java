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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.error.OutOfRangeException;
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;
import com.geophile.z.spatialobject.d2.Box;
import com.geophile.z.spatialobject.jts.JTSPolygon;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.fail;

public class BoxTableIndexScanIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        boxTable = createTable(
            "schema", "boxTable",
            "id int not null",
            "before int not null", // id mod 3
            "after int not null", // id mod 5
            "box_blob blob",
            "primary key(id)");
        createSpatialTableIndex("schema", "boxTable", "idx_box_blob", 0, 1, "box_blob");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        boxRowType = schema.tableRowType(table(boxTable));
        boxOrdinal = boxRowType.table().getOrdinal();
        boxBlobIndexRowType = indexType(boxTable, "box_blob");
        space = Spatial.createLatLonSpace();
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected int lookaheadQuantum() {
        return 1;
    }

    @Test
    public void testLoad()
    {
        loadDB();
        {
            // Check box_blob index
            Operator plan = indexScan_Default(boxBlobIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(boxBlobIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
/*
        {
            // Check (before, lat, lon) index
            Operator plan = indexScan_Default(beforeLatLonIndexRowType);
            long[][] expected = new long[zToId.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = new long[]{before(id), z, id};
            }
            compareRows(rows(beforeLatLonIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (lat, lon, after) index
            Operator plan = indexScan_Default(latLonAfterIndexRowType);
            long[][] expected = new long[zToId.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = new long[]{z, after(id), id};
            }
            compareRows(rows(latLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon, after) index
            Operator plan = indexScan_Default(beforeLatLonAfterIndexRowType);
            long[][] expected = new long[zToId.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = new long[]{before(id), z, after(id), id};
            }
            compareRows(rows(beforeLatLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
*/
    }

/*
    @Test
    public void testLoadAndRemove()
    {
        loadDB();
        {
            // Delete rows with odd ids
            for (Integer id : zToId.values()) {
                if ((id % 2) == 1) {
                    dml().deleteRow(session(), createNewRow(boxTable,
                                                            id,
                                                            before(id),
                                                            after(id),
                                                             lats.get(id),
                                                            lons.get(id)),
                                    false);
                }
            }
        }
        {
            // Check (lat, lon) index
            Operator plan = indexScan_Default(boxBlobIndexRowType);
            int rowsRemaining = zToId.size() / 2;
            if ((zToId.size() % 2) == 1) {
                rowsRemaining += 1;
            }
            long[][] expected = new long[rowsRemaining][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                // Only even ids should remain
                if ((id % 2) == 0) {
                    expected[r++] = new long[]{z, id};
                }
            }
            compareRows(rows(boxBlobIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon) index
            Operator plan = indexScan_Default(beforeLatLonIndexRowType);
            int rowsRemaining = zToId.size() / 2;
            if ((zToId.size() % 2) == 1) {
                rowsRemaining += 1;
            }
            long[][] expected = new long[rowsRemaining][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                // Only even ids should remain
                if ((id % 2) == 0) {
                    expected[r++] = new long[]{before(id), z, id};
                }
            }
            compareRows(rows(beforeLatLonIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (lat, lon, after) index
            Operator plan = indexScan_Default(latLonAfterIndexRowType);
            int rowsRemaining = zToId.size() / 2;
            if ((zToId.size() % 2) == 1) {
                rowsRemaining += 1;
            }
            long[][] expected = new long[rowsRemaining][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                // Only even ids should remain
                if ((id % 2) == 0) {
                    expected[r++] = new long[]{z, after(id), id};
                }
            }
            compareRows(rows(latLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon, after) index
            Operator plan = indexScan_Default(beforeLatLonAfterIndexRowType);
            int rowsRemaining = zToId.size() / 2;
            if ((zToId.size() % 2) == 1) {
                rowsRemaining += 1;
            }
            long[][] expected = new long[rowsRemaining][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                // Only even ids should remain
                if ((id % 2) == 0) {
                    expected[r++] = new long[]{before(id), z, after(id), id};
                }
            }
            compareRows(rows(beforeLatLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
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
                NewRow before = createNewRow(boxTable, id, before(id), after(id), lat, lon);
                NewRow after = createNewRow(boxTable, id, before(id), after(id), lat, lon.add(BigDecimal.ONE));
                long z = Spatial.shuffle(space, lat.doubleValue(), lon.doubleValue() + 1);
                zToId.put(z, id);
                dml().updateRow(session(), before, after, null);
            }
        }
        {
            // Check (lat, lon) index
            Operator plan = indexScan_Default(boxBlobIndexRowType);
            long[][] expected = new long[zToId.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = new long[]{z, id};
            }
            compareRows(rows(boxBlobIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon) index
            Operator plan = indexScan_Default(beforeLatLonIndexRowType);
            long[][] expected = new long[zToId.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = new long[]{before(id), z, id};
            }
            compareRows(rows(beforeLatLonIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (lat, lon, after) index
            Operator plan = indexScan_Default(latLonAfterIndexRowType);
            long[][] expected = new long[zToId.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = new long[]{z, after(id), id};
            }
            compareRows(rows(latLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon, after) index
            Operator plan = indexScan_Default(beforeLatLonAfterIndexRowType);
            long[][] expected = new long[zToId.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = new long[]{before(id), z, after(id), id};
            }
            compareRows(rows(beforeLatLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testSpatialQueryLatLon()
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
            Set<Integer> expected = new HashSet<>();
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
            // Get the query result using the (lat, lon) index
            Set<Integer> actual = new HashSet<>();
            IndexBound lowerLeft = new IndexBound(row(boxBlobIndexRowType, latLo, lonLo),
                                                  new SetColumnSelector(0, 1));
            IndexBound upperRight = new IndexBound(row(boxBlobIndexRowType, latHi, lonHi),
                                                   new SetColumnSelector(0, 1));
            IndexKeyRange box = IndexKeyRange.spatial(boxBlobIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(boxBlobIndexRowType, box, lookaheadQuantum());
            Cursor cursor = API.cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                assertSame(boxBlobIndexRowType.physicalRowType(), row.rowType());
                long z = getLong(row, 0);
                Integer expectedId = zToId.get(z);
                assertNotNull(expectedId);
                int id = getLong(row, 1).intValue();
                assertEquals(expectedId.intValue(), id);
                assertEquals(expectedHKey(id), row.hKey().toString());
                actual.add(id);
            }
            // There should be no false negatives
            assertTrue(actual.containsAll(expected));
        }
    }
*/

    private void loadDB()
    {
        int id = 0;
        final int MAX_Z = 4;
        long[] zs = new long[MAX_Z];
        for (long y = LAT_LO; y <= LAT_HI; y += DLAT) {
            for (long x = LON_LO; x < LON_HI; x += DLON) {
                JTSPolygon box = box(x, x + BOX_WIDTH, y, y + BOX_WIDTH);
                dml().writeRow(session(), createNewRow(boxTable, id, before(id),  after(id), box));
                Spatial.shuffle(space, box, zs);
                for (int i = 0; i < MAX_Z; i++) {
                    long z = zs[i];
                    zToId.add(z, id);
                    boxes.add(box);
                    zValues.add(z);
                    id++;
                }
            }
        }
    }

    private JTSPolygon box(double xLo, double xHi, double yLo, double yHi)
    {
        Coordinate[] coords = new Coordinate[5];
        coords[0] = new Coordinate(xLo, yLo);
        coords[1] = new Coordinate(xLo, yHi);
        coords[2] = new Coordinate(xHi, yHi);
        coords[3] = new Coordinate(xHi, yLo);
        coords[4] = coords[0];
        LinearRing ring = FACTORY.createLinearRing(coords);
        return new JTSPolygon(space, FACTORY.createPolygon(ring, null));
    }

    private long before(long id)
    {
        return id % 3;
    }

    private long after(long id)
    {
        return id % 5;
    }

    private Row[] rows(RowType rowType, long[][] x)
    {
        Row[] rows = new Row[x.length];
        for (int i = 0; i < x.length; i++) {
            long[] a = x[i];
            Object[] oa = new Object[a.length];
            for (int j = 0; j < a.length; j++) {
                oa[j] = a[j];
            }
            rows[i] = row(rowType, oa);
        }
        return rows;
    }

    private String expectedHKey(int id)
    {
        return String.format("{%s,(long)%s}", boxOrdinal, id);
    }

    private long[][] sort(long[][] a)
    {
        Arrays.sort(a,
                    new Comparator<long[]>()
                    {
                        @Override
                        public int compare(long[] x, long[] y)
                        {
                            for (int i = 0; i < x.length; i++) {
                                if (x[i] < y[i]) {
                                    return -1;
                                }
                                if (x[i] > y[i]) {
                                    return 1;
                                }
                            }
                            return 0;
                        }
                    });
        return a;
    }

    private void goodBox(int latLo, int latHi, int lonLo, int lonHi)
    {
        new Box(latLo, latHi, lonLo, lonHi);
    }

    private void badBox(int latLo, int latHi, int lonLo, int lonHi)
    {
        try {
            goodBox(latLo, latHi, lonLo, lonHi);
            fail();
        } catch (OutOfRangeException e) {
        }
    }

    private BigDecimal decimal(int x)
    {
        return new BigDecimal(x);
    }

    private static final int LAT_LO = -90;
    private static final int LAT_HI = 90;
    private static final int LON_LO = -180;
    private static final int LON_HI = 180;
    private static final int LAT_RANGE = LAT_HI - LAT_LO;
    private static final int LON_RANGE = LON_HI - LON_LO;
    private static final int DLAT = 10;
    private static final int DLON = 10;
    private static final int BOX_WIDTH = 2;
    private static final GeometryFactory FACTORY = new GeometryFactory();

    private int boxTable;
    private TableRowType boxRowType;
    private int boxOrdinal;
    private IndexRowType boxBlobIndexRowType;
    private IndexRowType beforeLatLonIndexRowType;
    private IndexRowType latLonAfterIndexRowType;
    private IndexRowType beforeLatLonAfterIndexRowType;
    private Space space;
    private ZToIdMapping zToId = new ZToIdMapping();
    List<JTSPolygon> boxes = new ArrayList<>(); // indexed by id
    List<Long> zValues = new ArrayList<>(); // indexed by id
    Random random = new Random(123456);
}
