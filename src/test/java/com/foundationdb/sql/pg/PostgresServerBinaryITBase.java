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

import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Ignore
public abstract class PostgresServerBinaryITBase extends PostgresServerSelectIT
{
    public PostgresServerBinaryITBase(String caseName, String sql, String expected, String error, String[] params) {
        super(caseName, sql, expected, error, params);
    }

    protected abstract boolean isBinaryTransfer();

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        Collection<Object[]> allCases = TestBase.sqlAndExpectedAndParams(RESOURCE_DIR);
        List<Object[]> typeCases = new ArrayList<>();
        for(Object[] a : allCases) {
            String caseName = (String)a[0];
            if(caseName.equals("types") || caseName.startsWith("types_a")) {
                typeCases.add(a);
            }
        }
        assert !typeCases.isEmpty() : "No types_a cases";
        return NamedParamsTestBase.namedCases(typeCases);
    }

    @Override
    protected String getConnectionURL() {
        // loglevel=2 is also useful for seeing what's really happening.
        return super.getConnectionURL() + "?prepareThreshold=1&binaryTransfer=" + isBinaryTransfer();
    }

    @Override
    protected boolean executeTwice() {
        return true;
    }

}
