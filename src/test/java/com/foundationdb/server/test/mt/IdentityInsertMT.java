/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.server.test.mt;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

public class IdentityInsertMT extends PostgresMTBase
{
    private static final Logger LOG = LoggerFactory.getLogger(IdentityInsertMT.class);

    private static final int TOTAL_STEPS = 150;
    private static final String TABLE_NAME = "t";
    private static final String DROP_STMT = "DROP TABLE IF EXISTS " + TABLE_NAME;
    private static final String CREATE_STMT = "CREATE TABLE " + TABLE_NAME + "(id SERIAL PRIMARY KEY, x INT)";
    private static final String INSERT_STMT_FMT = "INSERT INTO " + TABLE_NAME + "(x) VALUES (%d) RETURNING *";
    private static final String COMMIT_STMT = "COMMIT";
    private static final String ROLLBACK_STMT = "ROLLBACK";

    private final long seed = Long.parseLong(System.getProperty("fdbsql.test.seed", ""+System.currentTimeMillis()));
    private final Random random = new Random(seed);
    private final List<Connection> connections = new ArrayList<>();
    private final List<Statement> statements = new ArrayList<>();

    @Rule
    public final TestWatcher watcher = new TestWatcher() {
        protected void failed(Throwable e, Description description) {
            LOG.error("Failure with seed: {}", seed);
        }
    };

    @Before
    public void tearDown() {
        statements.clear();
        for(Connection c : connections) {
            try {
                c.close();
            } catch(SQLException e) {
                // Ignore
            }
        }
        connections.clear();
    }


    @Test
    public void oneConn() throws Exception {
        run(1);
    }

    @Test
    public void twoConn() throws Exception {
        run(2);
    }

    @Test
    public void tenConn() throws Exception {
        run(10);
    }

    private void run(int connCount) throws Exception {
        random.setSeed(seed);

        for(int i = 0; i < connCount; ++i) {
            Connection c = createConnection();
            c.setAutoCommit(false);
            connections.add(c);
            statements.add(c.createStatement());
        }

        statements.get(0).execute(DROP_STMT);
        statements.get(0).execute(CREATE_STMT);

        for(int i = 1; i <= TOTAL_STEPS; ++i) {
            for(int j = 0; j < statements.size(); ++j) {
                int v = random.nextInt(100);
                String stmt;
                if(v < 10) {
                    stmt = ROLLBACK_STMT;
                } else if(v < 30) {
                    stmt = COMMIT_STMT;
                } else {
                    stmt = String.format(INSERT_STMT_FMT, v);
                }
                try {
                    LOG.debug("conn{}: {}", j+1, stmt);
                    Statement s = statements.get(j);
                    if(s.execute(stmt)) {
                        ResultSet rs = s.getResultSet();
                        while(rs.next()) {
                            LOG.debug("  Inserted: ({},{})", rs.getInt(1), rs.getInt(2));
                        }
                        rs.close();
                    }
                } catch(SQLException e) {
                    if(!e.getSQLState().startsWith("40")) {
                        fail(String.format("Non-retryable error on step %d: (%s) %s\n", i, e.getSQLState(), e.getMessage()));
                    } else {
                        LOG.debug("  Retryable error: {}", e.getSQLState());
                    }
                }
            }
        }
    }
}
