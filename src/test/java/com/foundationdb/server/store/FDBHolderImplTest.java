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

package com.foundationdb.server.store;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.tuple.Tuple;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(NamedParameterizedRunner.class)
public final class FDBHolderImplTest
{
    private static Parameterization c(String dirString, Tuple expected) {
        String name = String.valueOf(dirString);
        if(name.isEmpty()) {
            name = "empty";
        }
        return Parameterization.create(name, dirString, expected);
    }

    @TestParameters
    public static Collection<Parameterization> types() throws Exception {
        return Arrays.asList(
            c(null, null),
            c("", Tuple.from()),
            c("sql", Tuple.from("sql")),
            c("sql/", Tuple.from("sql")),
            c("  pre", Tuple.from("pre")),
            c("post  ", Tuple.from("post")),
            c("  pre/post  ", Tuple.from("pre", "post")),
            c("foo/bar/zap", Tuple.from("foo", "bar", "zap")),
            c("alpha\\beta\\gamma", Tuple.from("alpha", "beta", "gamma")),
            c("a\\b/c\\\\d//e", Tuple.from("a", "b", "c", "d", "e"))
        );
    }


    private final String dirString;
    private final Tuple expected;

    public FDBHolderImplTest(String dirString, Tuple expected) {
        this.dirString = dirString;
        this.expected = expected;
    }

    @Test
    public void doCompare() {
        try {
            Tuple actual = Tuple.fromList(FDBHolderImpl.parseDirString(dirString));
            if(expected.size() != actual.size()) {
                fail(String.format("Tuple size mismatch: [%s] vs [%s]", expected.getItems(), actual.getItems()));
            }
            for(int i = 0; i < expected.size(); ++i) {
                Object e = expected.get(i);
                Object a = actual.get(i);
                assertEquals(String.format("tuple(%d)", i), e, a);
            }
        } catch(IllegalArgumentException e) {
            if(dirString != null) {
                throw e;
            }
            // else: expected
        }
    }
}
