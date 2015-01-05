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

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.error.OutOfRangeException;
import com.foundationdb.server.spatial.BoxLatLon;
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static java.lang.Math.abs;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class SpatialLatLonTableIndexScanIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        point = createTable(
            "schema", "point",
            "id int not null",
            "before int not null", // id mod 3
            "after int not null", // id mod 5
            "lat decimal(11, 7)",
            "lon decimal(11, 7)",
            "primary key(id)");
        createSpatialTableIndex("schema", "point", "lat_lon", 0, 2, "lat", "lon");
        createSpatialTableIndex("schema", "point", "before_lat_lon", 1, 2, "before", "lat", "lon");
        createSpatialTableIndex("schema", "point", "lat_lon_after", 0, 2, "lat", "lon", "after");
        createSpatialTableIndex("schema", "point", "before_lat_lon_after", 1, 2, "before", "lat", "lon", "after");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        pointRowType = schema.tableRowType(table(point));
        pointOrdinal = pointRowType.table().getOrdinal();
        latLonIndexRowType = indexType(point, "lat", "lon");
        beforeLatLonIndexRowType = indexType(point, "before", "lat", "lon");
        latLonAfterIndexRowType = indexType(point, "lat", "lon", "after");
        beforeLatLonAfterIndexRowType = indexType(point, "before", "lat", "lon", "after");
        space = Spatial.createLatLonSpace();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected int lookaheadQuantum()
    {
        return 1;
    }

    @Test
    public void testLoad()
    {
        loadDB();
        {
            // Check (lat, lon) index
            Operator plan = indexScan_Default(latLonIndexRowType);
            long[][] expected = zToId.toArray(new ZToIdMapping.ExpectedRowCreator()
            {
                @Override
                public long[] fields(long z, int id)
                {
                    return new long[]{z, id};
                }
            });
            compareRows(rows(latLonIndexRowType
                                 .physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon) index
            Operator plan = indexScan_Default(beforeLatLonIndexRowType);
            long[][] expected = zToId.toArray(new ZToIdMapping.ExpectedRowCreator()
            {
                @Override
                public long[] fields(long z, int id)
                {
                    return new long[]{before(id), z, id};
                }
            });
            compareRows(rows(beforeLatLonIndexRowType
                                 .physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (lat, lon, after) index
            Operator plan = indexScan_Default(latLonAfterIndexRowType);
            long[][] expected = zToId.toArray(new ZToIdMapping.ExpectedRowCreator()
            {
                @Override
                public long[] fields(long z, int id)
                {
                    return new long[]{z, after(id), id};
                }
            });
            compareRows(rows(latLonAfterIndexRowType
                                 .physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon, after) index
            Operator plan = indexScan_Default(beforeLatLonAfterIndexRowType);
            long[][] expected = zToId.toArray(new ZToIdMapping.ExpectedRowCreator()
            {
                @Override
                public long[] fields(long z, int id)
                {
                    return new long[]{before(id), z, after(id), id};
                }
            });
            compareRows(rows(beforeLatLonAfterIndexRowType
                                 .physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testLoadAndRemove()
    {
        loadDB();
        {
            // Delete rows with odd ids
            for (Integer id : zToId.ids()) {
                if ((id % 2) == 1) {
                    deleteRow(point, id, before(id), after(id), lats.get(id), lons.get(id));
                }
            }
        }
        {
            // Check (lat, lon) index
            Operator plan = indexScan_Default(latLonIndexRowType);
            int rowsRemaining = zToId.ids().size() / 2;
            if ((zToId.ids().size() % 2) == 1) {
                rowsRemaining += 1;
            }
            long[][] expected = zToId.toArray(rowsRemaining,
                                              new ZToIdMapping.ExpectedRowCreator()
                                              {
                                                  @Override
                                                  public long[] fields(long z, int id)
                                                  {
                                                      // Only even ids should remain
                                                      return (id % 2) == 0 ? new long[]{z, id} : null;
                                                  }
                                              });

            compareRows(rows(latLonIndexRowType
                                 .physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon) index
            Operator plan = indexScan_Default(beforeLatLonIndexRowType);
            int rowsRemaining = zToId.ids().size() / 2;
            if ((zToId.ids().size() % 2) == 1) {
                rowsRemaining += 1;
            }
            long[][] expected = zToId.toArray(rowsRemaining,
                                              new ZToIdMapping.ExpectedRowCreator()
                                              {
                                                  @Override
                                                  public long[] fields(long z, int id)
                                                  {
                                                      // Only even ids should remain
                                                      return (id % 2) == 0 ? new long[]{before(id), z, id} : null;
                                                  }
                                              });
            compareRows(rows(beforeLatLonIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (lat, lon, after) index
            Operator plan = indexScan_Default(latLonAfterIndexRowType);
            int rowsRemaining = zToId.ids().size() / 2;
            if ((zToId.ids().size() % 2) == 1) {
                rowsRemaining += 1;
            }
            long[][] expected = zToId.toArray(rowsRemaining,
                                              new ZToIdMapping.ExpectedRowCreator()
                                              {
                                                  @Override
                                                  public long[] fields(long z, int id)
                                                  {
                                                      // Only even ids should remain
                                                      return (id % 2) == 0 ? new long[]{z, after(id), id} : null;
                                                  }
                                              });
            compareRows(rows(latLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon, after) index
            Operator plan = indexScan_Default(beforeLatLonAfterIndexRowType);
            int rowsRemaining = zToId.ids().size() / 2;
            if ((zToId.ids().size() % 2) == 1) {
                rowsRemaining += 1;
            }
            long[][] expected = zToId.toArray(rowsRemaining,
                                              new ZToIdMapping.ExpectedRowCreator()
                                              {
                                                  @Override
                                                  public long[] fields(long z, int id)
                                                  {
                                                      // Only even ids should remain
                                                      return (id % 2) == 0 ? new long[]{before(id), z, after(id), id} : null;
                                                  }
                                              });
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
                Row before = row(point, id, before(id), after(id), lat, lon);
                Row after = row(point, id, before(id), after(id), lat, lon.add(BigDecimal.ONE));
                long z = Spatial.shuffle(space, lat.doubleValue(), lon.doubleValue() + 1);
                zToId.add(z, id);
                updateRow(before, after);
            }
        }
        {
            // Check (lat, lon) index
            Operator plan = indexScan_Default(latLonIndexRowType);
            long[][] expected = zToId.toArray(new ZToIdMapping.ExpectedRowCreator()
            {
                @Override
                public long[] fields(long z, int id)
                {
                    return new long[]{z, id};
                }
            });
            compareRows(rows(latLonIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon) index
            Operator plan = indexScan_Default(beforeLatLonIndexRowType);
            long[][] expected = zToId.toArray(new ZToIdMapping.ExpectedRowCreator()
            {
                @Override
                public long[] fields(long z, int id)
                {
                    return new long[]{before(id), z, id};
                }
            });
            compareRows(rows(beforeLatLonIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (lat, lon, after) index
            Operator plan = indexScan_Default(latLonAfterIndexRowType);
            long[][] expected = zToId.toArray(new ZToIdMapping.ExpectedRowCreator()
            {
                @Override
                public long[] fields(long z, int id)
                {
                    return new long[]{z, after(id), id};
                }
            });
            compareRows(rows(latLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            // Check (before, lat, lon, after) index
            Operator plan = indexScan_Default(beforeLatLonAfterIndexRowType);
            long[][] expected = zToId.toArray(new ZToIdMapping.ExpectedRowCreator()
            {
                @Override
                public long[] fields(long z, int id)
                {
                    return new long[]{before(id), z, after(id), id};
                }
            });
            compareRows(rows(beforeLatLonAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testSpatialQueryLatLon()
    {
        loadDB();
        final int TRIALS = 100;
        BigDecimal latLo;
        BigDecimal latHi;
        BigDecimal lonLo;
        BigDecimal lonHi;
        for (int i = 0; i < TRIALS; i++) {
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
            IndexBound lowerLeft = new IndexBound(row(latLonIndexRowType, latLo, lonLo),
                                                  new SetColumnSelector(0, 1));
            IndexBound upperRight = new IndexBound(row(latLonIndexRowType, latHi, lonHi),
                                                   new SetColumnSelector(0, 1));
            IndexKeyRange box = IndexKeyRange.spatialCoords(latLonIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(latLonIndexRowType, box, lookaheadQuantum());
            Cursor cursor = API.cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                assertSame(latLonIndexRowType.physicalRowType(), row.rowType());
                long z = getLong(row, 0);
                int id = getLong(row, 1).intValue();
                assertEquals(expectedHKey(id), row.hKey().toString());
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
        final int TRIALS = 100;
        BigDecimal latLo;
        BigDecimal latHi;
        BigDecimal lonLo;
        BigDecimal lonHi;
        for (int i = 0; i < TRIALS; i++) {
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
            // Get the query result
            Set<Integer> actual = new HashSet<>();
            IndexBound lowerLeft = new IndexBound(row(latLonIndexRowType, latLo, lonLo),
                                                  new SetColumnSelector(0, 1));
            IndexBound upperRight = new IndexBound(row(latLonIndexRowType, latHi, lonHi),
                                                   new SetColumnSelector(0, 1));
            IndexKeyRange box = IndexKeyRange.spatialCoords(latLonIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(latLonIndexRowType, box, lookaheadQuantum());
            Cursor cursor = API.cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                assertSame(latLonIndexRowType.physicalRowType(), row.rowType());
                long z = getLong(row, 0);
                int id = getLong(row, 1).intValue();
                assertEquals(expectedHKey(id), row.hKey().toString());
                actual.add(id);
            }
            // There should be no false negatives
            assertTrue(actual.containsAll(expected));
        }
    }

    @Test
    public void testHybridQueryLatLon()
    {
        loadDB();
        final int TRIALS = 100;
        BigDecimal latLo;
        BigDecimal latHi;
        BigDecimal lonLo;
        BigDecimal lonHi;
        for (int i = 0; i < TRIALS; i++) {
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
            // before = id mod 3, so try before = 0, 1, 2
            for (int before = 0; before <= 2; before++) {
                // Get the right answer
                Set<Integer> expected = new HashSet<>();
                for (int id = 0; id < lats.size(); id++) {
                    BigDecimal lat = lats.get(id);
                    BigDecimal lon = lons.get(id);
                    if (before(id) == before &&
                        latLo.compareTo(lat) <= 0 &&
                        lat.compareTo(latHi) <= 0 &&
                        lonLo.compareTo(lon) <= 0 &&
                        lon.compareTo(lonHi) <= 0) {
                        expected.add(id);
                    }
                }
                // Get the query result using the (before, lat, lon) index
                Set<Integer> actual = new HashSet<>();
                IndexBound lowerLeft = new IndexBound(row(beforeLatLonIndexRowType, before, latLo, lonLo),
                                                      new SetColumnSelector(0, 1, 2));
                IndexBound upperRight = new IndexBound(row(beforeLatLonIndexRowType, before, latHi, lonHi),
                                                       new SetColumnSelector(0, 1, 2));
                IndexKeyRange box = IndexKeyRange.spatialCoords(beforeLatLonIndexRowType, lowerLeft, upperRight);
                Operator plan = indexScan_Default(beforeLatLonIndexRowType, box, lookaheadQuantum());
                Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                cursor.openTopLevel();
                Row row;
                while ((row = cursor.next()) != null) {
                    assertSame(beforeLatLonIndexRowType.physicalRowType(), row.rowType());
                    int rowBefore = getLong(row, 0).intValue();
                    int rowId = getLong(row, 2).intValue();
                    assertEquals(before, rowBefore);
                    assertEquals(expectedHKey(rowId), row.hKey().toString());
                    actual.add(rowId);
                }
                // There should be no false negatives
                assertTrue(actual.containsAll(expected));
            }
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
            long zStart = Spatial.shuffle(space, queryLat.doubleValue(), queryLon.doubleValue());
            IndexBound zStartBound = new IndexBound(row(latLonIndexRowType, queryLat, queryLon),
                                                    new SetColumnSelector(0, 1));
            IndexKeyRange zStartRange = IndexKeyRange.around(latLonIndexRowType, zStartBound);
            Operator plan = indexScan_Default(latLonIndexRowType, zStartRange, lookaheadQuantum());
            Cursor cursor = API.cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            long previousDistance = Long.MIN_VALUE;
            int count = 0;
            while ((row = cursor.next()) != null) {
                assertSame(latLonIndexRowType.physicalRowType(), row.rowType());
                long zActual = getLong(row, 0);
                int id = getLong(row, 1).intValue();
                BigDecimal lat = lats.get(id);
                BigDecimal lon = lons.get(id);
                long zExpected = Spatial.shuffle(space, lat.doubleValue(), lon.doubleValue());
                assertEquals(zExpected, zActual);
                assertEquals(expectedHKey(id), row.hKey().toString());
                long distance = abs(zExpected - zStart);
                assertTrue(distance >= previousDistance);
                previousDistance = distance;
                count++;
            }
            assertEquals(zToId.ids().size(), count);
        }
    }

    @Test
    public void testHybridNearPoint()
    {
        loadDB();
        final int N = 100;
        for (int i = 0; i < N; i++) {
            BigDecimal queryLat = randomLat();
            BigDecimal queryLon = randomLon();
            long zStart = Spatial.shuffle(space, queryLat.doubleValue(), queryLon.doubleValue());
            for (int before = 0; before <= 2; before++) {
                // Expected
                SortedMap<Long, List<Integer>> distanceToId = new TreeMap<>();
                for (Map.Entry<Long, List<Integer>> entry : zToId) {
                    long z = entry.getKey();
                    for (Integer id : entry.getValue()) {
                        if (before(id) == before) {
                            long distance = abs(z - zStart);
                            List<Integer> ids = distanceToId.get(distance);
                            if (ids == null) {
                                ids = new ArrayList<>();
                                distanceToId.put(distance, ids);
                            }
                            ids.add(id);
                        }
                    }
                }
                List<Integer> expectedIdByDistance = new ArrayList<>();
                for (List<Integer> ids : distanceToId.values()) {
                    for (Integer id : ids) {
                        expectedIdByDistance.add(id);
                    }
                }
                // Actual
                IndexBound zStartBound =
                    new IndexBound(row(beforeLatLonIndexRowType, before,  queryLat, queryLon),
                                   new SetColumnSelector(0, 1, 2));
                IndexKeyRange zStartRange = IndexKeyRange.around(beforeLatLonIndexRowType, zStartBound);
                Operator plan = indexScan_Default(beforeLatLonIndexRowType, zStartRange, lookaheadQuantum());
                Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                cursor.openTopLevel();
                Row row;
                long previousDistance = Long.MIN_VALUE;
                List<Integer> actualIdByDistance = new ArrayList<>();
                while ((row = cursor.next()) != null) {
                    assertSame(beforeLatLonIndexRowType.physicalRowType(), row.rowType());
                    int beforeActual = getLong(row, 0).intValue();
                    assertEquals(before, beforeActual);
                    long zActual = getLong(row, 1);
                    int id = getLong(row, 2).intValue();
                    BigDecimal lat = lats.get(id);
                    BigDecimal lon = lons.get(id);
                    long zExpected = Spatial.shuffle(space, lat.doubleValue(), lon.doubleValue());
                    assertEquals(zExpected, zActual);
                    assertEquals(expectedHKey(id), row.hKey().toString());
                    long distance = abs(zExpected - zStart);
                    assertTrue(distance >= previousDistance);
                    previousDistance = distance;
                    actualIdByDistance.add(id);
                }
                Collections.sort(expectedIdByDistance);
                Collections.sort(actualIdByDistance);
                assertEquals(expectedIdByDistance, actualIdByDistance);
            }
        }
    }

    @Test
    public void testLongitudeBounds()
    {
        goodBox(0, 0, 0, 179);
        goodBox(0, 0, 0, 180);
        goodBox(0, 0, 0, 181);
        goodBox(0, 0, 0, 359);
        goodBox(0, 0, 0, 360);
        goodBox(0, 0, 0, 361);
        goodBox(0, 0, 0, 539);
        goodBox(0, 0, 0, 540);
        badBox(0, 0, 0, 541);

        goodBox(0, 0, 179, 0);
        goodBox(0, 0, 180, 0);
        goodBox(0, 0, 181, 0);
        goodBox(0, 0, 359, 0);
        goodBox(0, 0, 360, 0);
        goodBox(0, 0, 361, 0);
        goodBox(0, 0, 539, 0);
        goodBox(0, 0, 540, 0);
        badBox(0, 0, 541, 0);

        goodBox(0, 0, 0, -179);
        goodBox(0, 0, 0, -180);
        goodBox(0, 0, 0, -181);
        goodBox(0, 0, 0, -359);
        goodBox(0, 0, 0, -360);
        goodBox(0, 0, 0, -361);
        goodBox(0, 0, 0, -539);
        goodBox(0, 0, 0, -540);
        badBox(0, 0, 0, -541);

        goodBox(0, 0, -179, 0);
        goodBox(0, 0, -180, 0);
        goodBox(0, 0, -181, 0);
        goodBox(0, 0, -359, 0);
        goodBox(0, 0, -360, 0);
        goodBox(0, 0, -361, 0);
        goodBox(0, 0, -539, 0);
        goodBox(0, 0, -540, 0);
        badBox(0, 0, -541, 0);
    }

    @Test
    public void testLatitudeBounds()
    {
        goodBox(0, 89, 0, 0);
        goodBox(0, 90, 0, 0);
        goodBox(0, 91, 0, 0);
        goodBox(0, 181, 0, 0);
        goodBox(0, 361, 0, 0);
        goodBox(0, 449, 0, 0);
        goodBox(0, 450, 0, 0);
        badBox(0, 451, 0, 0);

        badBox(89, 0, 0, 0);
        badBox(90, 0, 0, 0);
        badBox(91, 0, 0, 0);

        badBox(0, -89, 0, 0);
        badBox(0, -90, 0, 0);
        badBox(0, -91, 0, 0);

        goodBox(-89, 0, 0, 0);
        goodBox(-90, 0, 0, 0);
        goodBox(-91, 0, 0, 0);
        goodBox(-181, 0, 0, 0);
        goodBox(-361, 0, 0, 0);
        goodBox(-449, 0, 0, 0);
        goodBox(-450, 0, 0, 0);
        badBox(-451, 0, 0, 0);
    }

    @Test
    public void testExceedingMaxLatitude()
    {
        loadDB();
        BigDecimal latLo = new BigDecimal(70);
        BigDecimal latHi = new BigDecimal(120);
        BigDecimal lonLo = new BigDecimal(40);
        BigDecimal lonHi = new BigDecimal(90);
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
        // Get the query result
        Set<Integer> actual = new HashSet<>();
        IndexBound lowerLeft = new IndexBound(row(latLonIndexRowType, latLo, lonLo),
                                              new SetColumnSelector(0, 0));
        IndexBound upperRight = new IndexBound(row(latLonIndexRowType, latHi, lonHi),
                                               new SetColumnSelector(0, 0));
        IndexKeyRange box = IndexKeyRange.spatialCoords(latLonIndexRowType, lowerLeft, upperRight);
        Operator plan = indexScan_Default(latLonIndexRowType, box, lookaheadQuantum());
        Cursor cursor = API.cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        Row row;
        while ((row = cursor.next()) != null) {
            assertSame(latLonIndexRowType.physicalRowType(), row.rowType());
            int id = getLong(row, 1).intValue();
            assertEquals(expectedHKey(id), row.hKey().toString());
            actual.add(id);
        }
        // There should be no false negatives
        assertTrue(actual.containsAll(expected));
    }

    @Test
    public void testExceedingMaxLongitude()
    {
/*        loadDB();
        BigDecimal latLo = new BigDecimal(-15);
        BigDecimal latHi = new BigDecimal(15);
        BigDecimal lonLo = new BigDecimal(160);
        BigDecimal lonHi = new BigDecimal(190);
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
        // Get the query result
        Set<Integer> actual = new HashSet<>();
        IndexBound lowerLeft = new IndexBound(row(latLonIndexRowType, latLo, lonLo),
                                              new SetColumnSelector(0, 0));
        IndexBound upperRight = new IndexBound(row(latLonIndexRowType, latHi, lonHi),
                                               new SetColumnSelector(0, 0));
        IndexKeyRange box = IndexKeyRange.spatial(latLonIndexRowType, lowerLeft, upperRight);
        Operator plan = indexScan_Default(latLonIndexRowType, box, lookaheadQuantum());
        Cursor cursor = API.cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        Row row;
        while ((row = cursor.next()) != null) {
            assertSame(latLonIndexRowType.physicalRowType(), row.rowType());
            long z = getLong(row, 0);
            Integer expectedId = zToId.get(z);
            assertNotNull(expectedId);
            int id = getLong(row, 1).intValue();
            assertEquals(expectedId.intValue(), id);
            assertEquals(expectedHKey(id), row.hKey().toString());
            actual.add(id);
        }
        // There should be no false negatives
        assertTrue(actual.containsAll(expected));*/
    }

    private void loadDB()
    {
        int id = 0;
        for (long y = LAT_LO; y <= LAT_HI; y += DLAT) {
            for (long x = LON_LO; x < LON_HI; x += DLON) {
                BigDecimal lat = new BigDecimal(y);
                BigDecimal lon = new BigDecimal(x);
                Row row = row(point, id, before(id), after(id), lat, lon);
                writeRow(row);
                long z = Spatial.shuffle(space, lat.doubleValue(), lon.doubleValue());
                zToId.add(z, id);
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

    private static long before(long id)
    {
        return id % 3;
    }

    private static long after(long id)
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
        return String.format("{%s,(long)%s}", pointOrdinal, id);
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
        BoxLatLon.newBox(latLo, latHi, lonLo, lonHi);
    }

    private void badBox(int latLo, int latHi, int lonLo, int lonHi)
    {
        try {
            goodBox(latLo, latHi, lonLo, lonHi);
            fail();
        } catch (OutOfRangeException e) {
            // Expected
        }
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
    private TableRowType pointRowType;
    private int pointOrdinal;
    private IndexRowType latLonIndexRowType;
    private IndexRowType beforeLatLonIndexRowType;
    private IndexRowType latLonAfterIndexRowType;
    private IndexRowType beforeLatLonAfterIndexRowType;
    private Space space;
    private ZToIdMapping zToId = new ZToIdMapping();
    List<BigDecimal> lats = new ArrayList<>(); // indexed by id
    List<BigDecimal> lons = new ArrayList<>(); // indexed by id
    List<Long> zs = new ArrayList<>(); // indexed by id
    Random random = new Random(123456);
}
