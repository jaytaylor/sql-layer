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
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;
import com.geophile.z.spatialobject.jts.JTS;
import com.geophile.z.spatialobject.jts.JTSSpatialObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertTrue;

public class BoxTableIndexScanIT extends OperatorITBase
{
    @Override
    protected boolean doAutoTransaction()
    {
        return false;
    }

    @Override
    protected void setupCreateSchema()
    {
        boxTable = createTable(
            "schema", "boxTable",
            "id int not null",
            "before int not null", // id mod 3
            "after int not null", // id mod 5
            "box blob",
            "primary key(id)");
        createSpatialTableIndex("schema", "boxTable", "idx_box", 0, 1, "box");
        createSpatialTableIndex("schema", "boxTable", "idx_before_box", 1, 1, "before", "box");
        createSpatialTableIndex("schema", "boxTable", "idx_box_after", 0, 1, "box", "after");
        createSpatialTableIndex("schema", "boxTable", "idx_before_box_after", 1, 1, "before", "box", "after");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        boxRowType = schema.tableRowType(table(boxTable));
        boxOrdinal = boxRowType.table().getOrdinal();
        boxIndexRowType = indexType(boxTable, "box");
        beforeBoxIndexRowType = indexType(boxTable, "before", "box");
        boxAfterIndexRowType = indexType(boxTable, "box", "after");
        beforeBoxAfterIndexRowType = indexType(boxTable, "before", "box", "after");
        space = Spatial.createLatLonSpace();
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
        try (TransactionContext t = new TransactionContext()) {
            // Check box index
            Operator plan = indexScan_Default(boxIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(boxIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (before, box) index
            Operator plan = indexScan_Default(beforeBoxIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{before(id), z, id};
                    }
                });
            compareRows(rows(beforeBoxIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (box, after) index
            Operator plan = indexScan_Default(boxAfterIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, after(id), id};
                    }
                });
            compareRows(rows(boxAfterIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (before, box, after) index
            Operator plan = indexScan_Default(beforeBoxAfterIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{before(id), z, after(id), id};
                    }
                });
            compareRows(rows(beforeBoxAfterIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testLoadAndRemove()
    {
        loadDB();
        try (TransactionContext t = new TransactionContext()) {
            // Delete rows with odd ids
            for (int id = 1; id < nIds; id += 2) {
                JTSSpatialObject box = boxes.get(id);
                deleteRow(row(boxTable, id, before(id), after(id), box), false);
            }
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check box index
            Operator plan = indexScan_Default(boxIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return
                            id % 2 == 0
                            ? new long[]{z, id}
                            : null;
                    }
                });
            compareRows(rows(boxIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (before, box) index
            Operator plan = indexScan_Default(beforeBoxIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return
                            id % 2 == 0
                            ? new long[]{before(id), z, id}
                            : null;
                    }
                });
            compareRows(rows(beforeBoxIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (box, after) index
            Operator plan = indexScan_Default(boxAfterIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return
                            id % 2 == 0
                            ? new long[]{z, after(id), id}
                            : null;
                    }
                });
            compareRows(rows(boxAfterIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (before, box, after) index
            Operator plan = indexScan_Default(beforeBoxAfterIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return
                            id % 2 == 0
                            ? new long[]{before(id), z, after(id), id}
                            : null;
                    }
                });
            compareRows(rows(beforeBoxAfterIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testLoadAndUpdate()
    {
        loadDB();
        int n = boxes.size();
        zToId.clear();
        try (TransactionContext t = new TransactionContext()) {
            // Shift boxes 1 cell up
            for (int id = 0; id < n; id++) {
                JTSSpatialObject box = boxes.get(id);
                // Envelope of box is the same as the box
                Envelope envelope = box.geometry().getEnvelopeInternal();
                double xLo = envelope.getMinX();
                double xHi = envelope.getMaxX();
                double yLo = envelope.getMinY();
                double yHi = envelope.getMaxY();
                JTSSpatialObject shiftedBox = box(xLo, xHi, yLo + 1, yHi + 1);
                Row oldRow = row(boxTable, id, before(id), after(id), box);
                Row newRow = row(boxTable, id, before(id), after(id), shiftedBox);
                recordZToId(id, shiftedBox);
                updateRow(oldRow, newRow);
            }
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check box index
            Operator plan = indexScan_Default(boxIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(boxIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (before, box) index
            Operator plan = indexScan_Default(beforeBoxIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{before(id), z, id};
                    }
                });
            compareRows(rows(beforeBoxIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (box, after) index
            Operator plan = indexScan_Default(boxAfterIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, after(id), id};
                    }
                });
            compareRows(rows(boxAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check (before, box, after) index
            Operator plan = indexScan_Default(beforeBoxAfterIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{before(id), z, after(id), id};
                    }
                });
            compareRows(rows(beforeBoxAfterIndexRowType.physicalRowType(), sort(expected)), cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testSpatialQuery()
    {
        final int ID_COLUMN = 1;
        loadDB();
        // dumpIndex(boxIndexRowType);
        final int QUERIES = 100;
        for (int q = 0; q < QUERIES; q++) {
            try (TransactionContext t = new TransactionContext()) {
                JTSSpatialObject queryBox = randomBox();
                // Get the right answer
                Set<Integer> expected = new HashSet<>();
                for (int id = 0; id < boxes.size(); id++) {
                    if (boxes.get(id).geometry().overlaps(queryBox.geometry())) {
                        expected.add(id);
                    }
                }
                // Get the query result using the box index
                Set<Integer> actual = new HashSet<>();
                IndexBound boxBound = new IndexBound(row(boxIndexRowType, queryBox),
                                                     new SetColumnSelector(0));
                IndexKeyRange box = IndexKeyRange.spatialObject(boxIndexRowType, boxBound);
                Operator plan = indexScan_Default(boxIndexRowType, box, lookaheadQuantum());
                Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                cursor.openTopLevel();
                Row row;
                while ((row = cursor.next()) != null) {
                    int id = getLong(row, ID_COLUMN).intValue();
                    actual.add(id);
                }
                // There should be no false negatives
                assertTrue(actual.containsAll(expected));
            }
        }
    }

    @Test
    public void testHybridQuery()
    {
        final int ID_COLUMN = 2;
        loadDB();
        // dumpIndex(beforeBoxIndexRowType);
        final int QUERIES = 100;
        for (int q = 0; q < QUERIES; q++) {
            try (TransactionContext t = new TransactionContext()) {
                JTSSpatialObject queryBox = randomBox();
                // before = id mod 3, so try before = 0, 1, 2
                for (int before = 0; before <= 2; before++) {
/*
                System.out.format("q = %d, before = %d, queryBox = %s\n",
                                  q, before, queryBox);
*/
                    // Get the right answer
                    Set<Integer> expected = new HashSet<>();
                    for (int id = 0; id < boxes.size(); id++) {
                        if (before(id) == before &&
                            boxes.get(id).geometry().overlaps(queryBox.geometry())) {
                            expected.add(id);
                        }
                    }
                    // Get the query result using the (before, box) index
                    Set<Integer> actual = new HashSet<>();
                    IndexBound boxBound = new IndexBound(row(beforeBoxIndexRowType, before, queryBox),
                                                         new SetColumnSelector(0, 1));
                    IndexKeyRange box = IndexKeyRange.spatialObject(beforeBoxIndexRowType, boxBound);
                    Operator plan = indexScan_Default(boxIndexRowType, box, lookaheadQuantum());
                    Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                    cursor.openTopLevel();
                    Row row;
                    while ((row = cursor.next()) != null) {
                        int id = getLong(row, ID_COLUMN).intValue();
                        actual.add(id);
                    }
                    // There should be no false negatives
                    List<Integer> actualSorted = new ArrayList<>(actual);
                    Collections.sort(actualSorted);
                    List<Integer> expectedSorted = new ArrayList<>(expected);
                    Collections.sort(expectedSorted);
/*
                System.out.println("Expected:");
                for (Integer e : expectedSorted) {
                    System.out.format("    %d: %d - %s\n", e, before(e), boxes.get(e));
                }
                System.out.println("Actual:");
                for (Integer a : actualSorted) {
                    System.out.format("    %d: %d - %s\n", a, before(a), boxes.get(a));
                }
*/
                    assertTrue(actual.containsAll(expected));
                }
            }
        }
    }


    private void loadDB()
    {
        try (TransactionContext t = new TransactionContext()) {
            int id = 0;
            for (long y = LAT_LO; y + BOX_WIDTH <= LAT_HI; y += DLAT) {
                for (long x = LON_LO; x + BOX_WIDTH < LON_HI; x += DLON) {
                    JTSSpatialObject box = box(y, y + BOX_WIDTH, x, x + BOX_WIDTH);
                    writeRow(session(), row(boxTable, id, before(id), after(id), box));
                    recordZToId(id, box);
                    boxes.add(box);
                    id++;
                }
            }
            nIds = id;
        }
    }

    private void recordZToId(int id, JTSSpatialObject box)
    {
        long[] zs = new long[box.maxZ()];
        Spatial.shuffle(space, box, zs);
        for (int i = 0; i < zs.length && zs[i] != Space.Z_NULL; i++) {
            long z = zs[i];
            zToId.add(z, id);
        }
    }

    private JTSSpatialObject randomBox()
    {
        double width = QUERY_WIDTH * random.nextDouble();
        double xLo = LAT_LO + (LAT_HI - LAT_LO - width) * random.nextDouble();
        double xHi = xLo + width;
        double height = QUERY_WIDTH * random.nextDouble();
        double yLo = LON_LO + (LON_HI - LON_LO - height) * random.nextDouble();
        double yHi = yLo + height;
        return box(xLo, xHi, yLo, yHi);
    }

    private JTSSpatialObject box(double xLo, double xHi, double yLo, double yHi)
    {
        Coordinate[] coords = new Coordinate[5];
        coords[0] = new Coordinate(xLo, yLo);
        coords[1] = new Coordinate(xLo, yHi);
        coords[2] = new Coordinate(xHi, yHi);
        coords[3] = new Coordinate(xHi, yLo);
        coords[4] = coords[0];
        return JTS.spatialObject(space, FACTORY.createPolygon(FACTORY.createLinearRing(coords), null));
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

    private void dumpIndex(IndexRowType indexRowType)
    {
        System.out.println("Boxes dump");
        for (int id = 0; id < boxes.size(); id++) {
            System.out.format("    %s: %s\n", id, boxes.get(id));
        }
        System.out.println();
        System.out.println("Index dump");
        Operator plan = indexScan_Default(indexRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        try {
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                System.out.format("    %s\n", row);
            }
        } finally {
            cursor.closeTopLevel();
        }
    }

    private static final int LAT_LO = -90;
    private static final int LAT_HI = 90;
    private static final int LON_LO = -180;
    private static final int LON_HI = 180;
    private static final int DLAT = 10;
    private static final int DLON = 10;
    private static final int BOX_WIDTH = 15; // Overlapping boxes, because it exceeds DLAT, DLON.
    private static final int QUERY_WIDTH = 30;
    private static final GeometryFactory FACTORY = new GeometryFactory();

    private int boxTable;
    private TableRowType boxRowType;
    private int boxOrdinal;
    private IndexRowType boxIndexRowType;
    private IndexRowType beforeBoxIndexRowType;
    private IndexRowType boxAfterIndexRowType;
    private IndexRowType beforeBoxAfterIndexRowType;
    private Space space;
    private ZToIdMapping zToId = new ZToIdMapping();
    List<JTSSpatialObject> boxes = new ArrayList<>();
    private int nIds;
    Random random = new Random(1234567);
}
