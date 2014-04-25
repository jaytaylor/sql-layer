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

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.sql.TestBase;
import org.junit.runner.RunWith;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

@RunWith(NamedParameterizedRunner.class)
public class PostgresServerBinaryTrueIT extends PostgresServerBinaryITBase
{
    public PostgresServerBinaryTrueIT(String caseName, String sql, String expected, String error, String[] params) {
        super(caseName, sql, expected, error, params);
        if ("types".equals(caseName)) {
            try {
                // Because of how java.sql.Timestamp is formatted.
                this.expected = TestBase.fileContents(new File(RESOURCE_DIR, "types.bexpected"));
            }
            catch (IOException ex) {
                fail(ex.getMessage());
            }
        }
    }

    @Override
    protected boolean isBinaryTransfer() {
        return true;
    }
}
