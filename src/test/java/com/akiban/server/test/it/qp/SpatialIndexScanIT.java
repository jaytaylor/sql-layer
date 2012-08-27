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

import com.akiban.ais.model.GroupTable;
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
import com.akiban.server.geophile.Space;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.util.*;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class SpatialIndexScanIT extends OperatorITBase
{
    @Before
    public void before()
    {
        point = createTable(
            "schema", "point",
            "id int not null",
            "x int",
            "y int",
            "primary key(id)");
        TableIndex xyIndex = createIndex("schema", "point", "xy", "x", "y");
        // TODO: Need to convert to DECIMAL lat, lon or add an
        // alternative Space for testing.
        xyIndex.setIndexMethod(Index.IndexMethod.Z_ORDER_LAT_LON);
        schema = new Schema(rowDefCache().ais());
        pointRowType = schema.userTableRowType(userTable(point));
        xyIndexRowType = indexType(point, "x", "y");
        space = new Space(LO, HI);
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
            Operator plan = indexScan_Default(xyIndexRowType);
            RowBase[] expected = new RowBase[zToId.size()];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = row(xyIndexRowType, z, (long) id);
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
                    dml().deleteRow(session(), createNewRow(point, id, xs.get(id), ys.get(id)));
                }
            }
        }
        {
            // Check index
            Operator plan = indexScan_Default(xyIndexRowType);
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
                    expected[r++] = row(xyIndexRowType, z, (long) id);
                }
            }
            compareRows(expected, cursor(plan, queryContext));
        }
    }

    @Test
    public void testLoadAndUpdate()
    {
        loadDB();
        int n = xs.size();
        zToId.clear();
        {
            // Increment y values
            for (int id = 0; id < n; id++) {
                Long x = xs.get(id);
                Long y = ys.get(id);
                NewRow before = createNewRow(point, id, x, y);
                NewRow after = createNewRow(point, id, x, y + 1);
                long z = space.shuffle(new long[]{x, y + 1});
                zToId.put(z, id);
                dml().updateRow(session(), before, after, null);
            }
        }
        {
            // Check index
            Operator plan = indexScan_Default(xyIndexRowType);
            RowBase[] expected = new RowBase[zToId.size()];
            int r = 0;
            for (Map.Entry<Long, Integer> entry : zToId.entrySet()) {
                long z = entry.getKey();
                int id = entry.getValue();
                expected[r++] = row(xyIndexRowType, z, (long) id);
            }
            compareRows(expected, cursor(plan, queryContext));
        }
    }

    @Test
    public void testSpatialQuery()
    {
        loadDB();
        Random random = new Random(987564);
        long xLo;
        long xHi;
        long yLo;
        long yHi;
        final int N = 100;
        for (int i = 0; i < N; i++) {
            xLo = abs(random.nextLong() % END_X);
            xHi = abs(random.nextLong() % END_X);
            if (xLo > xHi) {
                long swap = xLo;
                xLo = xHi;
                xHi = swap;
            }
            yLo = abs(random.nextLong() % END_Y);
            yHi = abs(random.nextLong() % END_Y);
            if (yLo > yHi) {
                long swap = yLo;
                yLo = yHi;
                yHi = swap;
            }
            // Get the right answer
            Set<Integer> expected = new HashSet<Integer>();
            for (int id = 0; id < xs.size(); id++) {
                long x = xs.get(id);
                long y = ys.get(id);
                if (xLo <= x && x <= xHi && yLo <= y && y <= yHi) {
                    expected.add(id);
                }
            }
            // Get the query result
            Set<Integer> actual = new HashSet<Integer>();
            IndexBound lowerLeft = new IndexBound(row(xyIndexRowType, xLo, yLo),
                                                  new SetColumnSelector(0, 1));
            IndexBound upperRight = new IndexBound(row(xyIndexRowType, xHi, yHi),
                                                   new SetColumnSelector(0, 1));
            IndexKeyRange box = IndexKeyRange.spatial(xyIndexRowType, lowerLeft, upperRight);
            Operator plan = indexScan_Default(xyIndexRowType, false, box);
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
            Row row;
            while ((row = cursor.next()) != null) {
                int id = (int) row.eval(1).getInt();
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
        Random random = new Random(123456);
        final int N = 100;
        long[] startingPoint = new long[2];
        for (int i = 0; i < N; i++) {
            startingPoint[0] = abs(random.nextLong() % END_X);
            startingPoint[1] = abs(random.nextLong() % END_Y);
            long zStart = space.shuffle(startingPoint);
            IndexBound zStartBound = new IndexBound(row(xyIndexRowType.physicalRowType(), space.shuffle(startingPoint)),
                                                    new SetColumnSelector(0));
            IndexKeyRange zStartRange = IndexKeyRange.spatial(xyIndexRowType, zStartBound, null);
            Operator plan = indexScan_Default(xyIndexRowType, false, zStartRange);
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
            Row row;
            long previousDistance = Long.MIN_VALUE;
            int count = 0;
            while ((row = cursor.next()) != null) {
                long zActual = row.eval(0).getLong();
                int id = (int) row.eval(1).getInt();
                long x = xs.get(id);
                long y = ys.get(id);
                long zExpected = space.shuffle(new long[]{x, y});
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
        for (long x = DX; x < END_X; x += DX) {
            for (long y = DY; y < END_Y; y += DY) {
                dml().writeRow(session(), createNewRow(point, id, x, y));
                long z = space.shuffle(new long[]{x, y});
                zToId.put(z, id);
                xs.add(x);
                ys.add(y);
                id++;
            }
        }
    }

    private static final long END_X = 1000L;
    private static final long END_Y = 1000L;
    private static final long[] LO = {0L, 0L};
    private static final long[] HI = {END_X - 1, END_Y - 1};
    private static final int DX = 100;
    private static final int DY = 100;

    private int point;
    private UserTableRowType pointRowType;
    private IndexRowType xyIndexRowType;
    private Space space;
    private Map<Long, Integer> zToId = new TreeMap<Long, Integer>();
    List<Long> xs = new ArrayList<Long>(); // indexed by id
    List<Long> ys = new ArrayList<Long>(); // indexed by id
}
