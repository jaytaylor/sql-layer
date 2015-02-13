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

import com.foundationdb.sql.embedded.EmbeddedJDBCITBase;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SpatialQueryDT extends EmbeddedJDBCITBase
{
    @Before
    public void setup() throws SQLException, ParseException
    {
        connection = null;
        Statement statement = null;
        PreparedStatement insert = null;
        try {
            connection = getConnection();
            statement = connection.createStatement();
            statement.execute("create table boxes(id int, box text, primary key(id))");
            String insertSQL = "insert into boxes values";
            for (int r = 0; r < ROWS_PER_INSERT; r++) {
                if (r > 0) {
                    insertSQL += ",";
                }
                insertSQL += "(?, ?)";
            }
            insert = connection.prepareStatement(insertSQL);
            int id = 0;
            while (id < N_BOXES) {
                for (int r = 0; r < ROWS_PER_INSERT; r++) {
                    insert.setInt(2 * r + 1, id++);
                    String box = randomBox(MAX_DATA_X, MAX_DATA_Y);
                    insert.setString(2 * r + 2, box);
                    boxes.add(geometry(box));
                }
                int updateCount = insert.executeUpdate();
                assertEquals(ROWS_PER_INSERT, updateCount);
            }
            // DON'T analyze the table. Run the first queries without the index (in theory).
        } finally {
            if (insert != null) {
                insert.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void spatialQueries() throws SQLException, ParseException
    {
        List<Integer> actual = new ArrayList<>();
        List<Integer> expected = new ArrayList<>();
        String selectSQL = "select id from boxes where geo_overlaps(geo_wkt(box), geo_wkt(?))";
        String addIndexSQL = "create index idx_box ON boxes(GEO_WKT(box))";
        String analyzeSQL = "alter table boxes all update statistics";
        PreparedStatement query = null;
        Statement indexing = null;
        try {
            query = connection.prepareStatement(selectSQL);
            indexing = connection.createStatement();
            for (int q = 0; q < N_QUERIES; q++) {
                if (q == N_QUERIES / 3) {
                    indexing.execute(addIndexSQL);
                    query = connection.prepareStatement(selectSQL);
                }
                if (q == 2 * N_QUERIES / 3) {
                    indexing.execute(analyzeSQL);
                    query = connection.prepareStatement(selectSQL);
                }
                String queryBox = randomBox(MAX_QUERY_X, MAX_QUERY_Y);
                // Actual
                actual.clear();
                query.setString(1, queryBox);
                try (ResultSet resultSet = query.executeQuery()) {
                    while (resultSet.next()) {
                        actual.add(resultSet.getInt(1));
                    }
                }
                // Expected
                expected.clear();
                Geometry queryGeo = geometry(queryBox);
                for (int id = 0; id < boxes.size(); id++) {
                    if (queryGeo.overlaps(boxes.get(id))) {
                        expected.add(id);
                    }
                }
                // Compare
                Collections.sort(actual);
                Collections.sort(expected);
                assertEquals(expected, actual);
            }
        } finally {
            if (query != null) {
                query.close();
            }
            if (indexing != null) {
                indexing.close();
            }
        }
    }

    private String randomBox(double maxX, double maxY)
    {
        double x = random.nextDouble() * LAT_SIZE + LAT_MIN;
        double y = random.nextDouble() * LON_SIZE + LON_MIN;
        double xSize = random.nextDouble() * maxX;
        double ySize = random.nextDouble() * maxY;
        double xLo = Math.max(x - xSize / 2, LAT_MIN);
        double xHi = Math.min(x + xSize / 2, LAT_MAX);
        double yLo = Math.max(y - ySize / 2, LON_MIN);
        double yHi = Math.min(y + ySize / 2, LON_MAX);
        String wkt = String.format("POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))",
                                   xLo, yLo,
                                   xLo, yHi,
                                   xHi, yHi,
                                   xHi, yLo,
                                   xLo, yLo);
        return wkt;
    }

    private static Geometry geometry(String wkt) throws ParseException
    {
        return WKT_READER.read(wkt);
    }

    private static final double LAT_MIN = -90;
    private static final double LAT_MAX = 90;
    private static final double LAT_SIZE = LAT_MAX - LAT_MIN;
    private static final double LON_MIN = -180;
    private static final double LON_MAX = 180;
    private static final double LON_SIZE = LON_MAX - LON_MIN;
    private static final double MAX_DATA_X = 10;
    private static final double MAX_DATA_Y = 20;
    private static final double MAX_QUERY_X = 10;
    private static final double MAX_QUERY_Y = 20;
    private static final int SEED = 101010101;
    private static final int N_BOXES = 100 * 1000;
    private static final int N_QUERIES = 60;
    private static final int ROWS_PER_INSERT = 50;
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final WKTReader WKT_READER = new WKTReader(GEOMETRY_FACTORY);

    private final Random random = new Random(SEED);
    private List<Geometry> boxes = new ArrayList<>();
    private Connection connection;
}
