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

package com.foundationdb.server.test.qt;

import com.foundationdb.sql.pg.PostgresService;
import com.foundationdb.sql.pg.PostgresServerITBase;

import java.sql.*;

public class PostgresBulkInsertQT extends BulkInsertQT
{
    @Override
    public Connection getConnection() throws SQLException {
        int port = getPostgresService().getPort();
        if (port <= 0) {
            throw new RuntimeException("fdbsql.postgres.port is not set.");
        }
        String url = String.format(PostgresServerITBase.CONNECTION_URL,
                                   getPostgresService().getHost(),
                                   port);
        return DriverManager.getConnection(url,
                                           PostgresServerITBase.USER_NAME,
                                           PostgresServerITBase.USER_PASSWORD);
    }

    protected PostgresService getPostgresService() {
        return serviceManager().getServiceByClass(PostgresService.class);
    }
}
