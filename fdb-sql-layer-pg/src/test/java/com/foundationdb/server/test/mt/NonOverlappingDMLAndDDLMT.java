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

package com.foundationdb.server.test.mt;

import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class NonOverlappingDMLAndDDLMT extends PostgresMTBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(BasicInfoSchemaTablesService.class, BasicInfoSchemaTablesServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    private static class SelectThread extends QueryThread
    {
        private final String schema;
        private final String table;

        public SelectThread(Connection conn, String schema, String table) throws SQLException {
            super("Select", schema, conn);
            this.schema = schema;
            this.table = table;
        }

        @Override
        protected int getLoopCount() {
            return 50;
        }

        @Override
        protected String[] getQueries() {
            return new String[] {
                "SELECT COUNT(*) FROM "+schema+"."+table,
                "SELECT * FROM "+schema+"."+table,
            };
        }
    }

    private static class DMLThread extends QueryThread
    {
        private final String schema;
        private final String table;

        public DMLThread(Connection conn, String schema, String table) throws SQLException {
            super("DML", schema, conn);
            this.schema = schema;
            this.table = table;
        }

        @Override
        protected int getLoopCount() {
            return 25;
        }

        @Override
        protected String[] getQueries() {
            return new String[] {
                "INSERT INTO "+schema+"."+table+" VALUES (10, 'ten')",
                "UPDATE "+schema+"."+table+" SET v='net' WHERE id=10",
                "DELETE FROM "+schema+"."+table+" WHERE id=10",
            };
        }
    }

    private static class DDLThread extends QueryThread
    {
        public DDLThread(Connection conn) throws SQLException {
            super("DDL", "test", conn);
        }

        @Override
        protected int getLoopCount() {
            return 25;
        }

        @Override
        protected String[] getQueries() {
            return new String[] {
                "CREATE TABLE t(ID INT NOT NULL PRIMARY KEY)",
                "DROP TABLE t"
            };
        }
    }

    @Test
    public void selectInfoSchema() throws SQLException {
        List<QueryThread> threads = Arrays.asList(
            new SelectThread(createConnection(), "information_schema", "tables"),
            new DDLThread(createConnection())
        );
        ThreadHelper.runAndCheck(30000, threads);
    }

    @Test
    public void selectRealTable() throws SQLException {
        String schema = "a_schema";
        String table = "t";
        int tid = createTable(schema, table, "id INT NOT NULL PRIMARY KEY, v VARCHAR(32)");
        for(int i = 1; i <= 5; ++i) {
            writeRow(tid, i, Integer.toString(i));
        }
        List<QueryThread> threads = Arrays.asList(
            new SelectThread(createConnection(), schema, table),
            new DDLThread(createConnection())
        );
        ThreadHelper.runAndCheck(30000, threads);
    }

    @Test
    public void dmlRealTable() throws SQLException {
        String schema = "a_schema";
        String table = "t";
        int tid = createTable(schema, table, "id INT NOT NULL PRIMARY KEY, v VARCHAR(32)");
        for(int i = 1; i <= 5; ++i) {
            writeRow(tid, i, Integer.toString(i));
        }
        List<QueryThread> threads = Arrays.asList(
            new DMLThread(createConnection(), schema, table),
            new DDLThread(createConnection())
        );
        ThreadHelper.runAndCheck(30000, threads);
    }
}
