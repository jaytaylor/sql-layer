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

package com.akiban.server.test.it.routines;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.loadableplan.LoadableOperator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.test.ExpressionGenerators;
import com.akiban.server.types.AkType;

import java.sql.Types;
import java.util.Arrays;

import static com.akiban.qp.operator.API.groupScan_Default;
import static com.akiban.qp.operator.API.project_Default;

/** A loadable operator plan.
 * <code><pre>
DROP TABLE test;
CREATE TABLE test(id INT PRIMARY KEY NOT NULL, value VARCHAR(10));
INSERT INTO test VALUES(1, 'aaa'), (2, 'bbb');
CALL sqlj.install_jar('target/akiban-server-1.4.3-SNAPSHOT-tests.jar', 'testjar', 0);
CREATE PROCEDURE test(IN n BIGINT) LANGUAGE java PARAMETER STYLE akiban_loadable_plan EXTERNAL NAME 'testjar:com.akiban.server.test.it.routines.TestPlan';
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
