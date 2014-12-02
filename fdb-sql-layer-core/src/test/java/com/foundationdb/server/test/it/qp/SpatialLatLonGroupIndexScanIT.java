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

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
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
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.server.types.value.ValueSources;
import com.geophile.z.Space;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static java.lang.Math.abs;
import static org.junit.Assert.*;

public class SpatialLatLonGroupIndexScanIT extends OperatorITBase
{
    @Override
    public void setupCreateSchema()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null", // 10x, x >= 0
            "pbefore int not null", // pid mod 3
            "plat decimal(11, 7)", // [-90, 90) in steps of 10
            "plon decimal(11, 7)", // [-180, 180) in steps of 10
            "primary key(pid)");
        child = createTable(
            "schema", "child",
            "cid int not null", // pid + x, x in [1, 9]
            "pid int",
            "cafter int not null", // cid mod 5
            "clat decimal(11, 7)", // plat + cid % 10
            "clon decimal(11, 7)", // plon + cid % 10
            "primary key(cid)",
            "grouping foreign key(pid) references parent(pid)");
        groupName = new TableName("schema", "parent");
        createSpatialGroupIndex(groupName, "pbefore_clat_clon_cafter",
                                1, Spatial.LAT_LON_DIMENSIONS, Index.JoinType.LEFT,
                                "parent.pbefore", "child.clat", "child.clon", "child.cafter");
        createSpatialGroupIndex(groupName, "pbefore_plat_plon_cafter",
                                1, Spatial.LAT_LON_DIMENSIONS, Index.JoinType.LEFT,
                                "parent.pbefore", "parent.plat", "parent.plon", "child.cafter");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        parentOrdinal = parentRowType.table().getOrdinal();
        childOrdinal = childRowType.table().getOrdinal();
        cSpatialIndexRowType = groupIndexType(groupName, "parent.pbefore", "child.clat", "child.clon", "child.cafter");
        pSpatialIndexRowType = groupIndexType(groupName, "parent.pbefore", "parent.plat", "parent.plon", "child.cafter");
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
            Operator plan = indexScan_Default(cSpatialIndexRowType);
            long[][] expected = new long[childZToCid.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : childZToCid.entrySet()) {
                long z = entry.getKey();
                int cid = entry.getValue();
                int pid = pid(cid);
                expected[r++] = new long[]{before(pid), z, after(cid), pid, cid};
            }
            compareRows(rows(cSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            Operator plan = indexScan_Default(pSpatialIndexRowType);
            long[][] expected = new long[parentZToPid.size() * CHILDREN_PER_PARENT][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : parentZToPid.entrySet()) {
                long z = entry.getKey();
                int pid = entry.getValue();
                for (int c = 1; c <= CHILDREN_PER_PARENT; c++) {
                    int cid = pid + c;
                    expected[r++] = new long[]{before(pid), z, after(cid), pid, cid};
                }
            }
            compareRows(rows(pSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testLoadAndRemove()
    {
        loadDB();
        List<Row> remainingRows = new ArrayList<>();
        {
            // Delete the first (1 + (pid % CHILDREN_PER_PARENT)) children of parent pid, and
            // keep track of the remaining rows.
            for (Integer pid : parentZToPid.values()) {
                int nChildrenToDelete = 1 + pid % CHILDREN_PER_PARENT;
                for (int c = 1; c <= CHILDREN_PER_PARENT; c++) {
                    int cid = pid + c;
                    Row row = row(child, cid, pid, after(cid), clats.get(cid), clons.get(cid));
                    if (c <= nChildrenToDelete) {
                        deleteRow(row);
                    } else {
                        remainingRows.add(row);
                    }
                }
            }
        }
        {
            Operator plan = indexScan_Default(cSpatialIndexRowType);
            long[][] expected = new long[remainingRows.size()][];
            int r = 0;
            for (Row row : remainingRows) {
                int cid = (Integer) ValueSources.toObject(row.value(0));
                int pid = (Integer) ValueSources.toObject(row.value(1));
                int after = (Integer) ValueSources.toObject(row.value(2));
                BigDecimal clat = (BigDecimal) ValueSources.toObject(row.value(3));
                BigDecimal clon = (BigDecimal) ValueSources.toObject(row.value(4));
                long z = Spatial.shuffle(space, clat.doubleValue(), clon.doubleValue());
                expected[r++] = new long[]{before(pid), z, after, pid, cid};
            }
            compareRows(rows(cSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            Operator plan = indexScan_Default(pSpatialIndexRowType);
            long[][] expected = new long[remainingRows.size()][];
            int r = 0;
            for (Row row : remainingRows) {
                int cid = (Integer) ValueSources.toObject(row.value(0));
                int pid = (Integer) ValueSources.toObject(row.value(1));
                int after = (Integer) ValueSources.toObject(row.value(2));
                BigDecimal plat = plats.get(pid);
                BigDecimal plon = plons.get(pid);
                long z = Spatial.shuffle(space, plat.doubleValue(), plon.doubleValue());
                expected[r++] = new long[]{before(pid), z, after, pid, cid};
            }
            compareRows(rows(pSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testLoadAndUpdate()
    {
        loadDB();
        List<Integer> pids = new ArrayList<>(parentZToPid.values());
        parentZToPid.clear();
        childZToCid.clear();
        {
            // Increment plon and clon values
            for (Integer pid : pids) {
                BigDecimal plat = plats.get(pid);
                BigDecimal plon = plons.get(pid);
                Row original = row(parent, pid, before(pid), plat, plon);
                Row updated = row(parent, pid, before(pid), plat, plon.add(BigDecimal.ONE));
                long z = Spatial.shuffle(space, plat.doubleValue(), plon.doubleValue() + 1);
                parentZToPid.put(z, pid);
                updateRow(original, updated);
                for (int c = 1; c <= CHILDREN_PER_PARENT; c++) {
                    Integer cid = pid + c;
                    BigDecimal clat = clats.get(cid);
                    BigDecimal clon = clons.get(cid);
                    original = row(child, cid, pid, after(cid), clat, clon);
                    updated = row(child, cid, pid, after(cid), clat, clon.add(BigDecimal.ONE));
                    updateRow(original, updated);
                    z = Spatial.shuffle(space, clat.doubleValue(), clon.doubleValue() + 1);
                    childZToCid.put(z, cid);
                }
            }
        }
        {
            Operator plan = indexScan_Default(cSpatialIndexRowType);
            long[][] expected = new long[childZToCid.size()][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : childZToCid.entrySet()) {
                long z = entry.getKey();
                int cid = entry.getValue();
                int pid = pid(cid);
                expected[r++] = new long[]{before(pid), z, after(cid), pid, cid};
            }
            compareRows(rows(cSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        {
            Operator plan = indexScan_Default(pSpatialIndexRowType);
            long[][] expected = new long[parentZToPid.size() * CHILDREN_PER_PARENT][];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : parentZToPid.entrySet()) {
                int pid = entry.getValue();
                long z = entry.getKey();
                for (int c = 1; c <= CHILDREN_PER_PARENT; c++) {
                    int cid = pid + c;
                    expected[r++] = new long[]{before(pid), z, after(cid), pid, cid};
                }
            }
            compareRows(rows(pSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testSpatialQueryPLatPLon()
    {
        // Find rows with random parent.before value and (plat, plon) inside random box.
        loadDB();
        final int N = 100;
        int beforeEQ;
        BigDecimal latLo;
        BigDecimal latHi;
        BigDecimal lonLo;
        BigDecimal lonHi;
        int nEmpty = 0;
        for (int i = 0; i < N; i++) {
            beforeEQ = randomBefore();
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
            Set<Integer> expectedCids = new HashSet<>();
            for (int pid : parentZToPid.values()) {
                int before = before(pid);
                BigDecimal lat = plats.get(pid);
                BigDecimal lon = plons.get(pid);
                if (before == beforeEQ &&
                    latLo.compareTo(lat) <= 0 &&
                    lat.compareTo(latHi) <= 0 &&
                    lonLo.compareTo(lon) <= 0 &&
                    lon.compareTo(lonHi) <= 0) {
                    for (int c = 1; c <= CHILDREN_PER_PARENT; c++) {
                        int cid = pid + c;
                        expectedCids.add(cid);
                    }
                }
            }
            if (expectedCids.isEmpty()) {
                nEmpty++;
            }
            // Get the query result using an index
            Set<Integer> actual = new HashSet<>();
            IndexBound lowerLeft = new IndexBound(row(pSpatialIndexRowType, beforeEQ, latLo, lonLo),
                                                  new SetColumnSelector(0, 1, 2));
            IndexBound upperRight = new IndexBound(row(pSpatialIndexRowType, beforeEQ, latHi, lonHi),
                                                   new SetColumnSelector(0, 1, 2));
            IndexKeyRange box = IndexKeyRange.spatialCoords(pSpatialIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(pSpatialIndexRowType, box, lookaheadQuantum());
            Cursor cursor = API.cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                assertSame(pSpatialIndexRowType.physicalRowType(), row.rowType());
                // Get row state
                int before = getLong(row, 0).intValue();
                long z = getLong(row, 1);
                int pid = getLong(row, 3).intValue();
                int cid = getLong(row, 4).intValue();
                // Check against expected
                assertEquals(beforeEQ, before);
                Integer expectedPid = parentZToPid.get(z);
                assertNotNull(expectedPid);
                assertEquals(expectedPid.intValue(), pid);
                assertTrue(cid >= expectedPid + 1 && cid <= expectedPid + CHILDREN_PER_PARENT);
                assertEquals(expectedHKey(pid, cid), row.hKey().toString());
                actual.add(cid);
            }
            // There should be no false negatives
            assertTrue(actual.containsAll(expectedCids));
        }
        // If there are too many empty results, we need to know about it, and try less restrictive queries.
        assertTrue(nEmpty < N * 0.2);
    }

    @Test
    public void testSpatialQueryCLatCLon()
    {
        // Find rows with random parent.before value and (clat, clon) inside random box.
        loadDB();
        final int N = 100;
        int beforeEQ;
        BigDecimal latLo;
        BigDecimal latHi;
        BigDecimal lonLo;
        BigDecimal lonHi;
        int nEmpty = 0;
        for (int i = 0; i < N; i++) {
            beforeEQ = randomBefore();
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
            Set<Integer> expectedCids = new HashSet<>();
            for (int cid : childZToCid.values()) {
                int before = before(pid(cid));
                BigDecimal lat = clats.get(cid);
                BigDecimal lon = clons.get(cid);
                if (before == beforeEQ &&
                    latLo.compareTo(lat) <= 0 &&
                    lat.compareTo(latHi) <= 0 &&
                    lonLo.compareTo(lon) <= 0 &&
                    lon.compareTo(lonHi) <= 0) {
                    expectedCids.add(cid);
                }
            }
            if (expectedCids.isEmpty()) {
                nEmpty++;
            }
            // Get the query result using an index
            Set<Integer> actual = new HashSet<>();
            IndexBound lowerLeft = new IndexBound(row(cSpatialIndexRowType, beforeEQ, latLo, lonLo),
                                                  new SetColumnSelector(0, 1, 2));
            IndexBound upperRight = new IndexBound(row(cSpatialIndexRowType, beforeEQ, latHi, lonHi),
                                                   new SetColumnSelector(0, 1, 2));
            IndexKeyRange box = IndexKeyRange.spatialCoords(cSpatialIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(cSpatialIndexRowType, box, lookaheadQuantum());
            Cursor cursor = API.cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                assertSame(cSpatialIndexRowType.physicalRowType(), row.rowType());
                // Get row state
                int before = getLong(row, 0).intValue();
                long z = getLong(row, 1);
                int pid = getLong(row, 3).intValue();
                int cid = getLong(row, 4).intValue();
                // Check against expected
                assertEquals(beforeEQ, before);
                Integer expectedCid = childZToCid.get(z);
                assertNotNull(expectedCid);
                assertEquals(expectedCid.intValue(), cid);
                assertEquals(pid(expectedCid), pid);
                assertEquals(expectedHKey(pid, cid), row.hKey().toString());
                actual.add(cid);
            }
            // There should be no false negatives
            assertTrue(actual.containsAll(expectedCids));
        }
        // If there are too many empty results, we need to know about it, and try less restrictive queries.
        assertTrue(nEmpty < N * 0.2);
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
            for (int beforeEQ = 0; beforeEQ <= 2; beforeEQ++) {
                // Expected
                SortedMap<Long, Integer> distanceToId = new TreeMap<>();
                for (Map.Entry<Long, Integer> entry : childZToCid.entrySet()) {
                    long z = entry.getKey();
                    int cid = entry.getValue();
                    if (before(pid(cid)) == beforeEQ) {
                        long distance = abs(z - zStart);
                        Integer replaced = distanceToId.put(distance, cid);
                        // TODO: Duplicate distances are possible
                        assertNull(replaced);
                    }
                }
                Collection<Integer> expectedIdByDistance = distanceToId.values();
                // Actual
                IndexBound zStartBound =
                    new IndexBound(row(cSpatialIndexRowType, beforeEQ,  queryLat, queryLon),
                                   new SetColumnSelector(0, 1, 2));
                IndexKeyRange zStartRange = IndexKeyRange.around(cSpatialIndexRowType, zStartBound);
                Operator plan = indexScan_Default(cSpatialIndexRowType, zStartRange, lookaheadQuantum());
                Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                cursor.openTopLevel();
                Row row;
                long previousDistance = Long.MIN_VALUE;
                Collection<Integer> actualIdByDistance = new ArrayList<>();
                while ((row = cursor.next()) != null) {
                    // Get row state
                    int before = getLong(row, 0).intValue();
                    long z = getLong(row, 1);
                    int pid = getLong(row, 3).intValue();
                    int cid = getLong(row, 4).intValue();
                    // Check against expected
                    assertSame(cSpatialIndexRowType.physicalRowType(), row.rowType());
                    assertEquals(beforeEQ, before);
                    BigDecimal clat = clats.get(cid);
                    BigDecimal clon = clons.get(cid);
                    long zExpected = Spatial.shuffle(space, clat.doubleValue(), clon.doubleValue());
                    assertEquals(zExpected, z);
                    Integer expectedCid = childZToCid.get(z);
                    assertNotNull(expectedCid);
                    assertEquals(expectedCid.intValue(), cid);
                    assertEquals(pid(expectedCid), pid);
                    assertEquals(expectedHKey(pid, cid), row.hKey().toString());
                    long distance = abs(zExpected - zStart);
                    assertTrue(distance >= previousDistance);
                    previousDistance = distance;
                    actualIdByDistance.add(cid);
                }
                assertEquals(new ArrayList<>(expectedIdByDistance),
                             new ArrayList<>(actualIdByDistance));
            }
        }
    }

    private void loadDB()
    {
        int pid = 0;
        for (long y = LAT_LO; y < LAT_HI; y += DLAT) {
            for (long x = LON_LO; x < LON_HI; x += DLON) {
                BigDecimal plat = new BigDecimal(y);
                BigDecimal plon = new BigDecimal(x);
                writeRow(parent, pid, before(pid), plat, plon);
                long parentZ = Spatial.shuffle(space, plat.doubleValue(), plon.doubleValue());
                parentZToPid.put(parentZ, pid);
                plats.put(pid, plat);
                plons.put(pid, plon);
                parentZs.add(parentZ);
                // System.out.println(String.format("parent  %016x -> %s", parentZ, pid));
                for (int cid = pid + 1; cid <= pid + CHILDREN_PER_PARENT; cid++) {
                    BigDecimal clat = clat(plat, cid);
                    BigDecimal clon = clon(plon, cid);
                    clats.put(cid, clat);
                    clons.put(cid, clon);
                    long childZ = Spatial.shuffle(space, clat.doubleValue(), clon.doubleValue());
                    childZToCid.put(childZ, cid);
                    // System.out.println(String.format("    child  %016x -> %s", childZ, cid));
                    writeRow(child, cid, pid, after(cid), clat, clon);
                }
                pid += 10;
            }
        }
    }

    private int randomBefore()
    {
        return random.nextInt(3);
    }

    private BigDecimal randomLat()
    {
        return new BigDecimal(random.nextDouble() * LAT_RANGE + LAT_LO);
    }

    private BigDecimal randomLon()
    {
        return new BigDecimal(random.nextDouble() * LON_RANGE + LON_LO);
    }

    private int before(int id)
    {
        return id % 3;
    }

    private int after(int id)
    {
        return id % 5;
    }

    private BigDecimal clat(BigDecimal plat, long cid)
    {
        return plat.add(new BigDecimal(cid % 10));
    }

    private BigDecimal clon(BigDecimal plon, long cid)
    {
        return plon.add(new BigDecimal(cid % 10));
    }

    private int pid(int cid)
    {
        return 10 * (cid / 10);
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

    private String expectedHKey(int pid, int cid)
    {
        return String.format("{%s,(long)%s,%s,(long)%s}", parentOrdinal, pid, childOrdinal, cid);
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

    private static final int LAT_LO = -90;
    private static final int LAT_HI = 90;
    private static final int LON_LO = -180;
    private static final int LON_HI = 180;
    private static final int LAT_RANGE = LAT_HI - LAT_LO;
    private static final int LON_RANGE = LON_HI - LON_LO;
    private static final int DLAT = 10;
    private static final int DLON = 10;
    private static final int CHILDREN_PER_PARENT = 3;

    private int parent;
    private int child;
    private TableName groupName;
    private TableRowType parentRowType;
    private TableRowType childRowType;
    private int parentOrdinal;
    private int childOrdinal;
    private IndexRowType cSpatialIndexRowType;
    private IndexRowType pSpatialIndexRowType;
    private Space space;
    private Map<Long, Integer> childZToCid = new TreeMap<>();
    private Map<Long, Integer> parentZToPid = new TreeMap<>();
    Map<Integer, BigDecimal> plats = new HashMap<>(); // indexed by pid
    Map<Integer, BigDecimal> plons = new HashMap<>(); // indexed by pid
    Map<Integer, BigDecimal> clats = new HashMap<>(); // indexed by cid
    Map<Integer, BigDecimal> clons = new HashMap<>(); // indexed by cid
    List<Long> parentZs = new ArrayList<>(); // indexed by id
    Random random = new Random(123456);
}
