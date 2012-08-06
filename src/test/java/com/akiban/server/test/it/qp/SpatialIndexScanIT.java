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
import com.akiban.ais.model.TableIndex;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.geophile.Space;
import com.akiban.util.StringsTest;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;

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
        xyIndex.spatialIndexDimensions(LO, HI);
        schema = new Schema(rowDefCache().ais());
        pointRowType = schema.userTableRowType(userTable(point));
        xyIndexRowType = indexType(point, "x", "y");
        group = groupTable(point);
        db = new NewRow[]{
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
    }

    @Test
    public void testLoad()
    {
        Map<Long, Integer> zToId = new TreeMap<Long, Integer>();
        Space space = new Space(LO, HI);
        {
            // Load
            int id = 0;
            for (long x = DX; x < END_X; x += DX) {
                for (long y = DY; y < END_Y; y += DY) {
                    dml().writeRow(session(), createNewRow(point, id, x, y));
                    long z = space.shuffle(new long[]{x, y});
                    zToId.put(z, id);
                    id++;
                }
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
    public void testLoadAndRemove()
    {
        Space space = new Space(LO, HI);
        Map<Long, Integer> zToId = new TreeMap<Long, Integer>();
        List<Long> xs = new ArrayList<Long>(); // indexed by id
        List<Long> ys = new ArrayList<Long>(); // indexed by id
        {
            // Load
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
        Space space = new Space(LO, HI);
        List<Long> xs = new ArrayList<Long>(); // indexed by id
        List<Long> ys = new ArrayList<Long>(); // indexed by id
        int n;
        {
            // Load
            int id = 0;
            for (long x = DX; x < END_X; x += DX) {
                for (long y = DY; y < END_Y; y += DY) {
                    dml().writeRow(session(), createNewRow(point, id, x, y));
                    xs.add(x);
                    ys.add(y);
                    id++;
                }
            }
            n = id;
        }
        Map<Long, Integer> zToId = new TreeMap<Long, Integer>();
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

    private static final long END_X = 1000L;
    private static final long END_Y = 1000L;
    private static final long[] LO = {0L, 0L};
    private static final long[] HI = {END_X - 1, END_Y - 1};
    private static final int DX = 100;
    private static final int DY = 100;

    private int point;
    private UserTableRowType pointRowType;
    private IndexRowType xyIndexRowType;
    private GroupTable group;
}
