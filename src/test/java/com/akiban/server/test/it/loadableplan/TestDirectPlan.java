/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.loadableplan;

import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.operator.QueryContext;

import java.sql.Types;

import java.util.Arrays;
import java.util.List;

/** A loadable direct object plan.
 * <code><pre>
java -jar jmxterm-1.0-alpha-4-uber.jar -l localhost:8082 -n <<EOF
run -b com.akiban:type=PostgresServer loadPlan `pwd`/`ls target/akiban-server-*-SNAPSHOT-tests.jar` com.akiban.server.test.it.loadableplan.TestDirectPlan
EOF
psql "host=localhost port=15432 sslmode=disable user=user password=pass" test <<EOF
call test_direct(10)
EOF
 * </pre></code> 
 */
public class TestDirectPlan extends LoadableDirectObjectPlan
{
    @Override
    public String name()
    {
        return "test_direct";
    }

    @Override
    public DirectObjectPlan plan()
    {
        return new DirectObjectPlan() {
                @Override
                public DirectObjectCursor cursor(QueryContext context) {
                    return new TestDirectObjectCursor(context);
                }
            };
    }

    public static class TestDirectObjectCursor extends DirectObjectCursor {
        private QueryContext context;
        private long i, n;

        public TestDirectObjectCursor(QueryContext context) {
            this.context = context;
        }

        @Override
        public void open() {
            i = 0;
            n = context.getValue(0).getLong();
        }

        @Override
        public List<Long> next() {
            if (i >= n)
                return null;
            return Arrays.asList(i++);
        }

        @Override
        public void close() {
        }
    }

    @Override
    public List<String> columnNames() {
        return NAMES;
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final List<String> NAMES = Arrays.asList("i");
    private static final int[] TYPES = new int[] { Types.INTEGER };
}
