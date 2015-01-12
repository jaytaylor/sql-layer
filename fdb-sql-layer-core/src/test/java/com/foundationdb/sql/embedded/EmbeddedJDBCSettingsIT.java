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

package com.foundationdb.sql.embedded;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import java.util.*;

public class EmbeddedJDBCSettingsIT extends EmbeddedJDBCITBase
{
    private static final String CONFIG_PREFIX = "fdbsql.embedded_jdbc.";

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> settings = new HashMap<>(super.startupConfigProperties());
        settings.put(CONFIG_PREFIX + "parserInfixLogical", "true");
        return settings;
    }

    @Test
    public void infixLogical() throws Exception {
        try (Connection connection = getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT true || false")) {
            assertTrue(rs.next());
            assertEquals("true", rs.getString(1)); // As opposed to truefalse.
        }
    }

}
