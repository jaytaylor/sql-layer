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

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.text.FullTextIndexService;
import com.foundationdb.server.service.text.ThrowingFullTextService;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ThrowingFullTextServiceIT extends PostgresServerITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(FullTextIndexService.class, ThrowingFullTextService.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void createFullTextIndex() throws Exception {
        createTable("test", "t", "id int not null primary key, v varchar(32) collate en_us_ci");
        Statement statement = getConnection().createStatement();
        try {
            statement.executeUpdate("CREATE INDEX ft ON t(FULL_TEXT(v))");
            fail("expected exception");
        } catch(SQLException e) {
            assertEquals("error code", ErrorCode.UNSUPPORTED_SQL.getFormattedValue(), e.getSQLState());
        }
    }
}
