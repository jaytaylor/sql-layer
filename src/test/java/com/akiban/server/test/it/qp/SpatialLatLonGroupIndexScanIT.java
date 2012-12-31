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
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.geophile.Space;
import com.akiban.server.geophile.SpaceLatLon;
import com.akiban.server.test.it.bugs.bug720768.GroupNameCollisionIT;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static java.lang.Math.abs;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
                                1, Space.LAT_LON_DIMENSIONS,
                                "parent.pbefore, child.clat, child.clon, child.cafter",
                                Index.JoinType.LEFT);
        createSpatialGroupIndex(groupName, "pbefore_plat_plon_cafter",
                                1, Space.LAT_LON_DIMENSIONS,
                                "parent.pbefore, parent.plat, parent.plon, child.cafter",
                                Index.JoinType.LEFT);
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        parentOrdinal = parentRowType.userTable().rowDef().getOrdinal();
        childOrdinal = childRowType.userTable().rowDef().getOrdinal();
        cSpatialIndexRowType = groupIndexType(groupName, "parent.pbefore", "child.clat", "child.clon", "child.cafter");
        pSpatialIndexRowType = groupIndexType(groupName, "parent.pbefore", "parent.plat", "parent.plon", "child.cafter");
        space = SpaceLatLon.create();
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
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
            compareRows(rows(cSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext));
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
            compareRows(rows(pSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext));
        }
    }

    @Test
    public void testLoadAndRemove()
    {
        loadDB();
        List<NewRow> remainingRows = new ArrayList<NewRow>();
        {
            // Delete the first (1 + (pid % CHILDREN_PER_PARENT)) children of parent pid, and
            // keep track of the remaining rows.
            for (Integer pid : parentZToPid.values()) {
                int nChildrenToDelete = 1 + pid % CHILDREN_PER_PARENT;
                for (int c = 1; c <= CHILDREN_PER_PARENT; c++) {
                    int cid = pid + c;
                    NewRow row = createNewRow(child, cid, pid, after(cid), clats.get(cid), clons.get(cid));
                    if (c <= nChildrenToDelete) {
                        dml().deleteRow(session(), row);
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
            for (NewRow row : remainingRows) {
                int cid = (Integer) row.get(0);
                int pid = (Integer) row.get(1);
                int after = (Integer) row.get(2);
                BigDecimal clat = (BigDecimal) row.get(3);
                BigDecimal clon = (BigDecimal) row.get(4);
                long z = space.shuffle(clat, clon);
                expected[r++] = new long[]{before(pid), z, after, pid, cid};
            }
            compareRows(rows(cSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext));
        }
        {
            Operator plan = indexScan_Default(pSpatialIndexRowType);
            long[][] expected = new long[remainingRows.size()][];
            int r = 0;
            for (NewRow row : remainingRows) {
                int cid = (Integer) row.get(0);
                int pid = (Integer) row.get(1);
                int after = (Integer) row.get(2);
                BigDecimal plat = plats.get(pid);
                BigDecimal plon = plons.get(pid);
                long z = space.shuffle(plat, plon);
                expected[r++] = new long[]{before(pid), z, after, pid, cid};
            }
            compareRows(rows(pSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext));
        }
    }

    @Test
    public void testLoadAndUpdate()
    {
        loadDB();
        List<Integer> pids = new ArrayList<Integer>(parentZToPid.values());
        parentZToPid.clear();
        childZToCid.clear();
        {
            // Increment plon and clon values
            for (Integer pid : pids) {
                BigDecimal plat = plats.get(pid);
                BigDecimal plon = plons.get(pid);
                NewRow original = createNewRow(parent, pid, before(pid), plat, plon);
                NewRow updated = createNewRow(parent, pid, before(pid), plat, plon.add(BigDecimal.ONE));
                long z = space.shuffle(plat, plon.add(BigDecimal.ONE));
                parentZToPid.put(z, pid);
                dml().updateRow(session(), original, updated, null);
                for (int c = 1; c <= CHILDREN_PER_PARENT; c++) {
                    Integer cid = pid + c;
                    BigDecimal clat = clats.get(cid);
                    BigDecimal clon = clons.get(cid);
                    original = createNewRow(child, cid, pid, after(cid), clat, clon);
                    updated = createNewRow(child, cid, pid, after(cid), clat, clon.add(BigDecimal.ONE));
                    dml().updateRow(session(), original, updated, null);
                    z = space.shuffle(clat, clon.add(BigDecimal.ONE));
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
            compareRows(rows(cSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext));
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
            compareRows(rows(pSpatialIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext));
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
            Set<Integer> expectedCids = new HashSet<Integer>();
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
            Set<Integer> actual = new HashSet<Integer>();
            IndexBound lowerLeft = new IndexBound(row(pSpatialIndexRowType, beforeEQ, latLo, lonLo),
                                                  new SetColumnSelector(0, 1, 2));
            IndexBound upperRight = new IndexBound(row(pSpatialIndexRowType, beforeEQ, latHi, lonHi),
                                                   new SetColumnSelector(0, 1, 2));
            IndexKeyRange box = IndexKeyRange.spatial(pSpatialIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(pSpatialIndexRowType, false, box);
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
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
            Set<Integer> expectedCids = new HashSet<Integer>();
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
            Set<Integer> actual = new HashSet<Integer>();
            IndexBound lowerLeft = new IndexBound(row(cSpatialIndexRowType, beforeEQ, latLo, lonLo),
                                                  new SetColumnSelector(0, 1, 2));
            IndexBound upperRight = new IndexBound(row(cSpatialIndexRowType, beforeEQ, latHi, lonHi),
                                                   new SetColumnSelector(0, 1, 2));
            IndexKeyRange box = IndexKeyRange.spatial(cSpatialIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(cSpatialIndexRowType, false, box);
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
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
            long zStart = space.shuffle(queryLat, queryLon);
            for (int beforeEQ = 0; beforeEQ <= 2; beforeEQ++) {
                // Expected
                SortedMap<Long, Integer> distanceToId = new TreeMap<Long, Integer>();
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
                Operator plan = indexScan_Default(cSpatialIndexRowType, false, zStartRange);
                Cursor cursor = API.cursor(plan, queryContext);
                cursor.open();
                Row row;
                long previousDistance = Long.MIN_VALUE;
                Collection<Integer> actualIdByDistance = new ArrayList<Integer>();
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
                    long zExpected = space.shuffle(clat, clon);
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
                assertEquals(new ArrayList<Integer>(expectedIdByDistance),
                             new ArrayList<Integer>(actualIdByDistance));
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
                dml().writeRow(session(), createNewRow(parent, pid, before(pid),  plat, plon));
                long parentZ = space.shuffle(SpaceLatLon.scaleLat(plat), SpaceLatLon.scaleLon(plon));
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
                    long childZ = space.shuffle(SpaceLatLon.scaleLat(clat), SpaceLatLon.scaleLon(clon));
                    childZToCid.put(childZ, cid);
                    // System.out.println(String.format("    child  %016x -> %s", childZ, cid));
                    dml().writeRow(session(), createNewRow(child, cid, pid, after(cid), clat, clon));
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

    private RowBase[] rows(RowType rowType, long[][] x)
    {
        RowBase[] rows = new RowBase[x.length];
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
    private UserTableRowType parentRowType;
    private UserTableRowType childRowType;
    private int parentOrdinal;
    private int childOrdinal;
    private IndexRowType cSpatialIndexRowType;
    private IndexRowType pSpatialIndexRowType;
    private SpaceLatLon space;
    private Map<Long, Integer> childZToCid = new TreeMap<Long, Integer>();
    private Map<Long, Integer> parentZToPid = new TreeMap<Long, Integer>();
    Map<Integer, BigDecimal> plats = new HashMap<Integer, BigDecimal>(); // indexed by pid
    Map<Integer, BigDecimal> plons = new HashMap<Integer, BigDecimal>(); // indexed by pid
    Map<Integer, BigDecimal> clats = new HashMap<Integer, BigDecimal>(); // indexed by cid
    Map<Integer, BigDecimal> clons = new HashMap<Integer, BigDecimal>(); // indexed by cid
    List<Long> parentZs = new ArrayList<Long>(); // indexed by id
    Random random = new Random(123456);
}
