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

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.loadableplan.LoadableOperator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.types.AkType;

import java.sql.Types;
import java.util.Arrays;

import static com.akiban.qp.operator.API.groupScan_Default;
import static com.akiban.qp.operator.API.project_Default;

/** A loadable operator plan.
 * <code><pre>
psql "host=localhost port=15432 sslmode=disable user=user password=pass" test <<EOF
DROP TABLE test;
CREATE TABLE test(id INT PRIMARY KEY NOT NULL, value VARCHAR(10));
INSERT INTO test VALUES(1, 'aaa'), (2, 'bbb');
EOF
java -jar jmxterm-1.0-alpha-4-uber.jar -l localhost:8082 -n <<EOF
run -b com.akiban:type=PostgresServer loadPlan `pwd`/`ls target/akiban-server-*-SNAPSHOT-tests.jar` com.akiban.server.test.it.loadableplan.TestPlan
EOF
psql "host=localhost port=15432 sslmode=disable user=user password=pass" test <<EOF
call test('666')
EOF
 * </pre></code> 
 */
public class TestPlan extends LoadableOperator
{
    @Override
    public String name()
    {
        return "test";
    }

    @Override
    public Operator plan()
    {
        // select id, value, $1 from test
        GroupTable groupTable = ais().getGroup("test").getGroupTable();
        UserTable testTable = ais().getUserTable("test", "test");
        RowType testRowType = schema().userTableRowType(testTable);
        return
            project_Default(
                groupScan_Default(groupTable),
                testRowType,
                Arrays.asList(Expressions.field(testRowType, 0),
                              Expressions.field(testRowType, 1),
                              Expressions.variable(AkType.LONG, 0)));
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final int[] TYPES = new int[]{Types.INTEGER, Types.VARCHAR, Types.INTEGER};
}
