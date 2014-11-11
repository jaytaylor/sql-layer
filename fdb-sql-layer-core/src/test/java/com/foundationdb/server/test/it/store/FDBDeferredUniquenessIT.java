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

package com.foundationdb.server.test.it.store;

import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.test.it.FDBITBase;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FDBDeferredUniquenessIT extends FDBITBase
{
    int tid;

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> props = new HashMap<>();
        props.putAll(super.startupConfigProperties());

        props.put("fdbsql.fdb.defer_uniqueness_checks", "true");
        return props;
    }

    @Before
    public void setup() {
        tid = createTable("test", "t1", 
                          "id int not null primary key",
                          "s varchar(10)");
    }

    @Test
    public void unique() {
        writeRow(tid, 1L, "fred");
        writeRow(tid, 2L, "wilma");
    }

    @Test(expected=DuplicateKeyException.class)
    public void notunique() {
        writeRow(tid, 1L, "fred");
        writeRow(tid, 1L, "barney");
    }

}
