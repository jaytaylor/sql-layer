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
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;
import com.geophile.z.spatialobject.d2.Box;
import com.geophile.z.spatialobject.jts.JTS;
import com.geophile.z.spatialobject.jts.JTSSpatialObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
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
            compareRows(rows(boxBlobIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testLoadAndRemove()
    {
        loadDB();
        {
            // Delete rows with odd ids
            for (int id = 1; id < nIds; id += 2) {
                JTSSpatialObject box = boxes.get(id);
                deleteRow(row(boxTable, id, before(id), after(id), box), false);
            }
        }
        {
            // Check box_blob index
            Operator plan = indexScan_Default(boxBlobIndexRowType);
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
            compareRows(rows(boxBlobIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testLoadAndUpdate()
    {
        loadDB();
        int n = boxes.size();
        zToId.clear();
        {
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
    }

    @Test
    public void testSpatialQueryLatLon()
    {
        loadDB();
        // dumpIndex();
        final int QUERIES = 100;
        for (int q = 0; q < QUERIES; q++) {
            JTSSpatialObject queryBox = randomBox();
            // Get the right answer
            Set<Integer> expected = new HashSet<>();
            for (int id = 0; id < boxes.size(); id++) {
                if (boxes.get(id).geometry().overlaps(queryBox.geometry())) {
                    expected.add(id);
                }
            }
            // Get the query result using the box_blob index
            Set<Integer> actual = new HashSet<>();
            IndexBound boxBound = new IndexBound(row(boxBlobIndexRowType, queryBox),
                                                 new SetColumnSelector(0));
            IndexKeyRange box = IndexKeyRange.spatial(boxBlobIndexRowType, boxBound, boxBound);
            Operator plan = indexScan_Default(boxBlobIndexRowType, box, lookaheadQuantum());
            Cursor cursor = API.cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                int id = getLong(row, 1).intValue();
                actual.add(id);
            }
            // There should be no false negatives
            assertTrue(actual.containsAll(expected));
        }
    }

    private void loadDB()
    {
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

    private void dumpIndex()
    {
        System.out.println("Index dump");
        Operator plan = indexScan_Default(boxBlobIndexRowType);
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
    private static final int LAT_RANGE = LAT_HI - LAT_LO;
    private static final int LON_RANGE = LON_HI - LON_LO;
    private static final int DLAT = 10;
    private static final int DLON = 10;
    private static final int BOX_WIDTH = 15; // Overlapping boxes, because it exceeds DLAT, DLON.
    private static final int QUERY_WIDTH = 30;
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
    List<JTSSpatialObject> boxes = new ArrayList<>();
    private int nIds;
    Random random = new Random(1234567);
}
