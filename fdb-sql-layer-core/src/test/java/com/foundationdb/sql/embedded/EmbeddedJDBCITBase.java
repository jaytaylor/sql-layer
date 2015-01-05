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

package com.foundationdb.sql.embedded;

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmbeddedJDBCITBase extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        // JDBC service is not in test-services.
        return super.serviceBindingsProvider()
            .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:default:connection";

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
    }

    protected List<List<?>> sql(String sql) {
        try (Connection conn = getConnection();
             Statement statement = conn.createStatement()) {
            if (!statement.execute(sql))
                return null;
            List<List<?>> results = new ArrayList<>();
            try (ResultSet rs = statement.getResultSet()) {
                int ncols = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>(ncols);
                    for (int i = 0; i < ncols; ++i)
                        row.add(rs.getObject(i+1));
                    results.add(row);
                }
            }
            if (statement.getMoreResults())
                throw new RuntimeException("multiple ResultSets for SQL: " + sql);
            return results;
        }
        catch (Exception ex) {
            throw new RuntimeException("while executing SQL: " + sql, ex);
        }
    }

}
