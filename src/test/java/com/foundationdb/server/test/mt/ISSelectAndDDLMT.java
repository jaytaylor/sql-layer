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
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Ignore("Disabled due to AIS generation vs RowType issue")
public class ISSelectAndDDLMT extends PostgresMTBase
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
        public SelectThread(Connection conn) throws SQLException {
            super("Select", "information_schema", conn);
        }

        @Override
        protected int getLoopCount() {
            return 100;
        }

        @Override
        protected String[] getQueries() {
            return new String[] {
                "SELECT COUNT(*) FROM information_schema.tables",
                "SELECT * FROM information_schema.tables"
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
    public void test() throws SQLException {
        List<QueryThread> threads = Arrays.asList(
            new SelectThread(createConnection()),
            new DDLThread(createConnection())
        );
        ThreadHelper.runAndCheck(30000, threads);
    }
}
