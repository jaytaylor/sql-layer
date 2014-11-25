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

package com.foundationdb.server.service.security;

import com.foundationdb.sql.pg.PostgresService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class PostgresSecurityIT extends SecurityServiceITBase
{

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = super.startupConfigProperties();
        properties.put("fdbsql.postgres.login", "md5");
        return properties;
    }

    private Connection openPostgresConnection(String user, String password) 
            throws Exception {
        int port = serviceManager().getServiceByClass(PostgresService.class).getPort();
        String host = serviceManager().getServiceByClass(PostgresService.class).getHost();
        String url = String.format("jdbc:fdbsql://%s:%d/%s", host, port, user);
        return DriverManager.getConnection(url, user, password);
    }

    @Test(expected = SQLException.class)
    public void postgresUnauthenticated() throws Exception {
        openPostgresConnection(null, null).close();
    }

    @Test
    public void postgresAuthenticated() throws Exception {
        Connection conn = openPostgresConnection("user1", "password");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM utable");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs.close();
        stmt.execute("DROP TABLE utable");
        stmt.close();
        conn.close();
    }

    @Test(expected = SQLException.class)
    public void postgresBadUser() throws Exception {
        openPostgresConnection("user2", "whatever").close();
    }

    @Test(expected = SQLException.class)
    public void postgresBadPassword() throws Exception {
        openPostgresConnection("user1", "nope").close();
    }

    @Test(expected = SQLException.class)
    public void postgresWrongSchema() throws Exception {
        Connection conn = openPostgresConnection("user1", "password");
        Statement stmt = conn.createStatement();
        stmt.executeQuery("SELECT id FROM user2.utable");
    }

    @Test(expected = SQLException.class)
    public void postgresWrongSchemaDDL() throws Exception {
        Connection conn = openPostgresConnection("user1", "password");
        Statement stmt = conn.createStatement();
        stmt.executeQuery("DROP TABLE user2.utable");
    }

}
