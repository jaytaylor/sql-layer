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

import com.foundationdb.sql.embedded.EmbeddedJDBCITBase;
import com.foundationdb.util.MicroBenchmark;
import com.vividsolutions.jts.io.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SpatialQueryPerformanceDT extends EmbeddedJDBCITBase
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
                    if (id % 1000 == 0) {
                        System.out.format("Inserted %d rows\n", id);
                    }
                }
                int updateCount = insert.executeUpdate();
                assertEquals(ROWS_PER_INSERT, updateCount);
            }
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
    public void spatialQueries() throws Exception
    {
        SpatialQueryBenchmark benchmark = new SpatialQueryBenchmark();
        report("No index", benchmark.run());
        benchmark.addIndex();
        report("With index", benchmark.run());
        benchmark.analyze();
        report("Analyzed", benchmark.run());
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

    private void report(String label, double nsec)
    {
        System.out.format("%s: %f msec\n", label, nsec / 1000000);
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
    private static final int ROWS_PER_INSERT = 50;
    private static final int BENCHMARK_HISTORY_SIZE = 10;
    private static final double BENCHMARK_VARIATION = 0.1;
    private static final String ADD_INDEX = "create index idx_box ON boxes(GEO_WKT(box))";
    private static final String ANALYZE = "alter table boxes all update statistics";
    private static final String SPATIAL_QUERY = "select id from boxes where geo_overlaps(geo_wkt(box), geo_wkt('%s'))";

    private final Random random = new Random(SEED);
    private Connection connection;

    private class SpatialQueryBenchmark extends MicroBenchmark
    {
        @Override
        public void beforeAction() throws SQLException
        {
            query = String.format(SPATIAL_QUERY, randomBox(MAX_QUERY_X, MAX_QUERY_Y));
            statement = connection.createStatement();
        }

        @Override
        public void afterAction() throws SQLException
        {
            statement.close();
        }

        @Override
        public void action() throws SQLException, ParseException
        {
            try (ResultSet resultSet = statement.executeQuery(query)) {
                while (resultSet.next()) {
                    resultSet.getInt(1);
                }
            }
        }

        public void addIndex() throws SQLException
        {
            statement.execute(ADD_INDEX);
        }

        public void analyze() throws SQLException
        {
            statement.execute(ANALYZE);
        }

        SpatialQueryBenchmark() throws SQLException
        {
            super(BENCHMARK_HISTORY_SIZE, BENCHMARK_VARIATION);
        }

        private String query;
        private Statement statement;
    }
}
