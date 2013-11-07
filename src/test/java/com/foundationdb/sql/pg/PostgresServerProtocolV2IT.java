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

package com.foundationdb.sql.pg;

import org.junit.Test;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Map;

public class PostgresServerProtocolV2IT extends PostgresServerITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(PostgresServerProtocolV2IT.class);
    }

    @Override
    protected String getConnectionURL() {
        return super.getConnectionURL() + "?protocolVersion=2";
    }
    
    @Test
    public void checkRejection() throws Exception {
        try {
            getConnection();
        }
        catch (SQLException ex) {
            if (ex.getMessage().indexOf("Unsupported protocol version 2") < 0) {
                fail("message was not readable: " + ex.getMessage());
            }
            return;
        }
        forgetConnection();
        fail("Expected connection to be rejected.");
    }

}
