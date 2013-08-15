/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.test.it.routines;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.loadableplan.LoadableOperator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.AkType;

import java.sql.Types;
import java.util.Arrays;

import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.project_Default;

/** A loadable operator plan.
 * <code><pre>
DROP TABLE test;
CREATE TABLE test(id INT PRIMARY KEY NOT NULL, value VARCHAR(10));
INSERT INTO test VALUES(1, 'aaa'), (2, 'bbb');
CALL sqlj.install_jar('target/foundationdb-sql-layer-2.0.0-SNAPSHOT-tests.jar', 'testjar', 0);
CREATE PROCEDURE test(IN n BIGINT) LANGUAGE java PARAMETER STYLE foundationdb_loadable_plan EXTERNAL NAME 'testjar:com.foundationdb.server.test.it.routines.TestPlan';
CALL test(666);
 * </pre></code> 
 */
public class TestPlan extends LoadableOperator
{
    @Override
    public Operator plan()
    {
        // select id, value, $1 from test
        Group group = ais().getGroup("test");
        UserTable testTable = ais().getUserTable("test", "test");
        RowType testRowType = schema().userTableRowType(testTable);
        return
            project_Default(
                groupScan_Default(group),
                testRowType,
                Arrays.asList(ExpressionGenerators.field(testRowType, 0),
                              ExpressionGenerators.field(testRowType, 1),
                              ExpressionGenerators.variable(AkType.LONG, 0)));
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final int[] TYPES = new int[]{Types.INTEGER, Types.VARCHAR, Types.INTEGER};
}
