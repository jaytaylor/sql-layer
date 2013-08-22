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

package com.foundationdb.sql;

import com.foundationdb.junit.Parameterization;

import org.junit.Ignore;

import java.util.Collection;
import java.util.ArrayList;

@Ignore
public class NamedParamsTestBase extends TestBase
{
    protected NamedParamsTestBase() {
    }

    protected NamedParamsTestBase(String caseName, String sql, String expected, String error) {
      super(caseName, sql, expected, error);
    }

    /** Given method args whose first one is caseName, make named parameterizations. */
    public static Collection<Parameterization> namedCases(Collection<Object[]> margs) {
        Collection<Parameterization> result = new ArrayList<>(margs.size());
        for (Object[] args : margs) {
            String caseName = (String)args[0];
            result.add(Parameterization.create(caseName, args));
        }
        return result;
    }

}
