/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursorSpatial_InBox;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.server.spatial.TestRecord;
import com.foundationdb.server.spatial.TreeIndex;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.WrappingByteSource;
import com.geophile.z.Pair;
import com.geophile.z.Record;
import com.geophile.z.Space;
import com.geophile.z.SpatialIndex;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;
import com.geophile.z.space.SpaceImpl;
import com.geophile.z.spatialobject.d2.Box;
import com.geophile.z.spatialobject.jts.JTS;
import com.geophile.z.spatialobject.jts.JTSSpatialObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpatialObjectsIndexIT extends OperatorITBase
{
    @Override
    protected boolean doAutoTransaction()
    {
        return false;
    }

    @Override
    protected void setupCreateSchema()
    {
        table = createTable(
            "schema", "table",
            "id int not null",
            "lat decimal(10, 5)",
            "lon decimal(10, 5)",
            "wkb blob", // POINT($LAT $LON)
            "wkt text", // POINT($LAT $LON)
            "primary key(id)");
        createSpatialTableIndex("schema", "table", "idx_lat_lon", "GEO_LAT_LON", 0, 2, "lat", "lon");
        createSpatialTableIndex("schema", "table", "idx_wkt", "GEO_WKT", 0, 1, "wkt");
        createSpatialTableIndex("schema", "table", "idx_wkb", "GEO_WKB", 0, 1, "wkb");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        TableRowType tableRowType = schema.tableRowType(table(table));
        group = tableRowType.table().getGroup();
        latLonIndexRowType = indexType(table, "lat", "lon");
        wktIndexRowType = indexType(table, "wkt");
        wkbIndexRowType = indexType(table, "wkb");
        space = Spatial.createLatLonSpace();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected int lookaheadQuantum()
    {
        return 1;
    }

    @Test
    public void testLoad() throws ParseException
    {
        loadDB();
        try (TransactionContext t = new TransactionContext()) {
            // Check table
            Operator plan = groupScan_Default(group);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            try {
                cursor.openTopLevel();
                AbstractRow row;
                int id = 0;
                while ((row = (AbstractRow) cursor.next()) != null) {
                    JTSSpatialObject jtsSpatialObject = points.get(id);
                    Point point = (Point) jtsSpatialObject.geometry();
                    assertEquals(id, row.value(0).getInt32());
                    // lat/lon
                    assertEquals(point.getX(), toDouble(row.value(1)), 0);
                    assertEquals(point.getY(), toDouble(row.value(2)), 0);
                    // wkb
                    assertEquals(jtsSpatialObject, wkbToSpatialObject(row.value(3)));
                    // wkt
                    assertEquals(jtsSpatialObject, wktToSpatialObject(row.value(4)));
                    id++;
                }
            } finally {
                cursor.closeTopLevel();
            }
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check wkt index
            Operator plan = indexScan_Default(wktIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(wktIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check wkb index
            Operator plan = indexScan_Default(wkbIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(wkbIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check lat/lon index
            Operator plan = indexScan_Default(latLonIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(wktIndexRowType.physicalRowType(), sort(expected)),
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
                JTSSpatialObject jtsSpatialObject = points.get(id);
                Point point = (Point) jtsSpatialObject.geometry();
                deleteRow(row(table, id, point.getX(), point.getY(), jtsSpatialObject, jtsSpatialObject), false);
            }
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check wkt index
            Operator plan = indexScan_Default(wktIndexRowType);
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
            compareRows(rows(wktIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check wkb index
            Operator plan = indexScan_Default(wkbIndexRowType);
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
            compareRows(rows(wkbIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check lat/lon index
            Operator plan = indexScan_Default(latLonIndexRowType);
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
            compareRows(rows(latLonIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testLoadAndUpdate()
    {
        loadDB();
        int n = points.size();
        zToId.clear();
        try (TransactionContext t = new TransactionContext()) {
            // Shift 1 cell up
            for (int id = 0; id < n; id++) {
                JTSSpatialObject jtsSpatialObject = points.get(id);
                Point point = (Point) jtsSpatialObject.geometry();
                double x = point.getX();
                double y = point.getY();
                double shiftedY = y == Spatial.MAX_LON ? y - 1 : y + 1;
                JTSSpatialObject shiftedPoint = point(x, shiftedY);
                Row oldRow = row(table, id, x, y, jtsSpatialObject, jtsSpatialObject);
                Row newRow = row(table, id, x, shiftedY, shiftedPoint, shiftedPoint);
                recordZToId(id, shiftedPoint);
                updateRow(oldRow, newRow);
            }
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check wkt index
            Operator plan = indexScan_Default(wktIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(wktIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check wkb index
            Operator plan = indexScan_Default(wkbIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(wkbIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check lat/lon index
            Operator plan = indexScan_Default(latLonIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return new long[]{z, id};
                    }
                });
            compareRows(rows(latLonIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testSpatialQuery() throws IOException, InterruptedException
    {
        final int ID_COLUMN = 1;
        loadDB();
        final int QUERIES = 100;
        for (int q = 0; q < QUERIES; q++) {
            try (TransactionContext t = new TransactionContext()) {
                JTSSpatialObject queryBox = randomBox();
                Set<Integer> expected;
                {
                    // Get the right answer
                    expected = new HashSet<>();
                    for (int id = 0; id < points.size(); id++) {
                        if (points.get(id).geometry().overlaps(queryBox.geometry())) {
                            expected.add(id);
                        }
                    }
                }
                List<SpatialJoinEvent> expectedEvents = new ArrayList<>();
                {
                    // Get the expected access pattern
                    TestRecordFactory recordFactory = new TestRecordFactory();
                    // data
                    int id = 0;
                    TreeIndex dataIndex = new TreeIndex();
                    SpatialIndex<TestRecord> dataSpatialIndex = SpatialIndex.newSpatialIndex(SPACE, dataIndex);
                    for (double lat = Spatial.MIN_LAT; lat <= Spatial.MAX_LAT; lat += DELTA_LAT) {
                        for (double lon = Spatial.MIN_LON; lon <= Spatial.MAX_LON; lon += DELTA_LON) {
                            com.geophile.z.spatialobject.d2.Point point = new com.geophile.z.spatialobject.d2.Point(lat, lon);
                            dataSpatialIndex.add(point, recordFactory.initialize(point, id++));
                        }
                    }
                    // query
                    TreeIndex queryIndex = new TreeIndex();
                    SpatialIndex<TestRecord> querySpatialIndex = SpatialIndex.newSpatialIndex(SPACE, queryIndex);
                    Envelope envelope = queryBox.geometry().getEnvelopeInternal();
                    Box box = new Box(envelope.getMinX(),
                                      envelope.getMaxX(),
                                      envelope.getMinY(),
                                      envelope.getMaxY());
                    querySpatialIndex.add(box, recordFactory.initialize(box, 0), IndexCursorSpatial_InBox.MAX_Z);
                    // spatial join
                    SpatialJoin spatialJoin =
                        SpatialJoin.newSpatialJoin(SpatialJoin.Duplicates.INCLUDE,
                                                   null,
                                                   new SpatialJoinObserver(Operand.QUERY, expectedEvents),
                                                   new SpatialJoinObserver(Operand.DATA, expectedEvents));
                    Iterator<Pair<TestRecord, TestRecord>> iterator = spatialJoin.iterator(querySpatialIndex, dataSpatialIndex);
                    while (iterator.hasNext()) {
                        iterator.next();
                    }
                }
                { // WKT
                    // Set up collection of spatial join events
                    List<SpatialJoinEvent> actualEvents = new ArrayList<>();
                    IndexCursorSpatial_InBox.SPATIAL_JOIN_LEFT_OBSERVER = new SpatialJoinObserver(Operand.QUERY, actualEvents);
                    IndexCursorSpatial_InBox.SPATIAL_JOIN_RIGHT_OBSERVER = new SpatialJoinObserver(Operand.DATA, actualEvents);
                    // Get the query result using the wkt index
                    Set<Integer> actual = new HashSet<>();
                    IndexBound boxBound = new IndexBound(row(wktIndexRowType, queryBox),
                                                         new SetColumnSelector(0));
                    IndexKeyRange box = IndexKeyRange.spatialObject(wktIndexRowType, boxBound);
                    Operator plan = indexScan_Default(wktIndexRowType, box, lookaheadQuantum());
                    Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                    cursor.openTopLevel();
                    Row row;
                    while ((row = cursor.next()) != null) {
                        int id = getLong(row, ID_COLUMN).intValue();
                        actual.add(id);
                    }
                    // There should be no false negatives
                    assertTrue(actual.containsAll(expected));
                    // Check access pattern
                    assertEquals(expectedEvents, actualEvents);
                }
                { // WKB
                    // Set up collection of spatial join events
                    List<SpatialJoinEvent> actualEvents = new ArrayList<>();
                    IndexCursorSpatial_InBox.SPATIAL_JOIN_LEFT_OBSERVER = new SpatialJoinObserver(Operand.QUERY, actualEvents);
                    IndexCursorSpatial_InBox.SPATIAL_JOIN_RIGHT_OBSERVER = new SpatialJoinObserver(Operand.DATA, actualEvents);
                    // Get the query result using the wkb index
                    Set<Integer> actual = new HashSet<>();
                    IndexBound boxBound = new IndexBound(row(wkbIndexRowType, queryBox),
                                                         new SetColumnSelector(0));
                    IndexKeyRange box = IndexKeyRange.spatialObject(wkbIndexRowType, boxBound);
                    Operator plan = indexScan_Default(wkbIndexRowType, box, lookaheadQuantum());
                    Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                    cursor.openTopLevel();
                    Row row;
                    while ((row = cursor.next()) != null) {
                        int id = getLong(row, ID_COLUMN).intValue();
                        actual.add(id);
                    }
                    // There should be no false negatives
                    assertTrue(actual.containsAll(expected));
                    // Check access pattern
                    assertEquals(expectedEvents, actualEvents);
                }
                { // LAT/LON
                    // Set up collection of spatial join events
                    List<SpatialJoinEvent> actualEvents = new ArrayList<>();
                    IndexCursorSpatial_InBox.SPATIAL_JOIN_LEFT_OBSERVER = new SpatialJoinObserver(Operand.QUERY, actualEvents);
                    IndexCursorSpatial_InBox.SPATIAL_JOIN_RIGHT_OBSERVER = new SpatialJoinObserver(Operand.DATA, actualEvents);
                    // Get the query result using the lat/lon index
                    Set<Integer> actual = new HashSet<>();
                    IndexBound boxBound = new IndexBound(row(latLonIndexRowType, queryBox),
                                                         new SetColumnSelector(0));
                    IndexKeyRange box = IndexKeyRange.spatialObject(latLonIndexRowType, boxBound);
                    Operator plan = indexScan_Default(latLonIndexRowType, box, lookaheadQuantum());
                    Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                    cursor.openTopLevel();
                    Row row;
                    while ((row = cursor.next()) != null) {
                        int id = getLong(row, ID_COLUMN).intValue();
                        actual.add(id);
                    }
                    // There should be no false negatives
                    assertTrue(actual.containsAll(expected));
                    // Check access pattern
                    assertEquals(expectedEvents, actualEvents);
                }
            }
        }
    }

    private void loadDB()
    {
        try (TransactionContext t = new TransactionContext()) {
            int id = 0;
            for (long lat = LAT_LO; lat <= LAT_HI; lat += DELTA_LAT) {
                for (long lon = LON_LO; lon <= LON_HI; lon += DELTA_LON) {
                    JTSSpatialObject point = point(lat, lon);
                    writeRow(session(), row(table, id, lat, lon, point, point));
                    recordZToId(id, point);
                    points.add(point);
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

    private JTSSpatialObject point(double lat, double lon)
    {
        return JTS.spatialObject(space, FACTORY.createPoint(new Coordinate(lat, lon)));
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
        Operator plan = indexScan_Default(indexRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        try {
            cursor.openTopLevel();
            IndexRow row;
            while ((row = (IndexRow) cursor.next()) != null) {
                System.out.format("    %s\n", SpaceImpl.formatZ(row.z()));
            }
        } finally {
            cursor.closeTopLevel();
        }
    }

    private SpatialObject wkbToSpatialObject(ValueSource valueSource) throws ParseException
    {
        return Spatial.deserializeWKB(space, ((WrappingByteSource) ValueSources.toObject(valueSource)).toByteSubarray());
    }

    private SpatialObject wktToSpatialObject(ValueSource valueSource) throws ParseException
    {
        return Spatial.deserializeWKT(space, ValueSources.toStringSimple(valueSource));
    }

    private double toDouble(ValueSource valueSource)
    {
        BigDecimalWrapper bigDecimalWrapper = (BigDecimalWrapper) valueSource.getObject();
        return bigDecimalWrapper.asBigDecimal().doubleValue();
    }

    private static final Space SPACE = Spatial.createLatLonSpace();
    private static final int LAT_LO = (int) Spatial.MIN_LAT;
    private static final int LAT_HI = (int) Spatial.MAX_LAT;
    private static final int LON_LO = (int) Spatial.MIN_LON;
    private static final int LON_HI = (int) Spatial.MAX_LON;
    private static final int DELTA_LAT = 10;
    private static final int DELTA_LON = 10;
    private static final int QUERY_WIDTH = 30;
    private static final GeometryFactory FACTORY = new GeometryFactory();

    private int table;
    private Group group;
    private IndexRowType wktIndexRowType;
    private IndexRowType wkbIndexRowType;
    private IndexRowType latLonIndexRowType;
    private Space space;
    private ZToIdMapping zToId = new ZToIdMapping();
    List<JTSSpatialObject> points = new ArrayList<>();
    private int nIds;
    Random random = new Random(1234567);

    private static class TestRecordFactory implements Record.Factory<TestRecord>
    {
        @Override
        public TestRecord newRecord()
        {
            return new TestRecord(spatialObject, id);
        }

        public TestRecordFactory initialize(SpatialObject spatialObject, int id)
        {
            this.spatialObject = spatialObject;
            this.id = id;
            return this;
        }

        private SpatialObject spatialObject;
        private int id;
    }

    private static class SpatialJoinObserver extends SpatialJoin.InputObserver
    {
        @Override
        public void enter(long z)
        {
            events.add(new SpatialJoinEvent(operand, SpatialJoinEventType.ENTER, z));
            // System.out.format("%s: ENTER %s\n", operand, SpaceImpl.formatZ(z));
        }

        @Override
        public void exit(long z)
        {
            events.add(new SpatialJoinEvent(operand, SpatialJoinEventType.EXIT, z));
            // System.out.format("%s: EXIT  %s\n", operand, SpaceImpl.formatZ(z));
        }

        @Override
        public void randomAccess(com.geophile.z.Cursor cursor, long z)
        {
            events.add(new SpatialJoinEvent(operand, SpatialJoinEventType.RANDOM_ACCESS, z));
            // System.out.format("%s:     GOTO %s\n", operand, SpaceImpl.formatZ(z));
        }

        @Override
        public void sequentialAccess(com.geophile.z.Cursor cursor, long zRandomAccess, Record record)
        {
            events.add(new SpatialJoinEvent(operand,
                                            SpatialJoinEventType.SEQUENTIAL_ACCESS,
                                            record == null ? Space.Z_NULL : record.z()));
/*
            System.out.format("%s:     NEXT %s -> %s\n",
                              operand,
                              SpaceImpl.formatZ(zRandomAccess),
                              record == null ? null : SpaceImpl.formatZ(record.z()));
*/
        }

        SpatialJoinObserver(Operand operand, List<SpatialJoinEvent> events)
        {
            this.operand = operand;
            this.events = events;
        }

        private final Operand operand;
        private final List<SpatialJoinEvent> events;
    }

    enum Operand
    {
        QUERY, DATA
    }

    enum SpatialJoinEventType
    {
        RANDOM_ACCESS, SEQUENTIAL_ACCESS, ENTER, EXIT
    }

    private static class SpatialJoinEvent
    {
        @Override
        public String toString()
        {
            return String.format("%s: %s %s",
                                 operand == Operand.DATA ? "DATA " : "QUERY", SpaceImpl.formatZ(z), eventType);
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = false;
            if (obj.getClass() == this.getClass()) {
                SpatialJoinEvent that = (SpatialJoinEvent) obj;
                eq =
                    this.operand == that.operand &&
                    this.eventType == that.eventType &&
                    this.z == that.z;
            }
            return eq;
        }

        public SpatialJoinEvent(Operand operand, SpatialJoinEventType eventType, long z)
        {
            this.operand = operand;
            this.eventType = eventType;
            this.z = z;
        }

        private final Operand operand;
        private final SpatialJoinEventType eventType;
        private final long z;
    }}
