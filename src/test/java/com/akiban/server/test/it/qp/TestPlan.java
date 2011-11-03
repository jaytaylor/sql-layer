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

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.std.Expressions;

import java.sql.Types;
import java.util.Arrays;

import static com.akiban.qp.operator.API.groupScan_Default;
import static com.akiban.qp.operator.API.project_Default;

public class TestPlan extends LoadablePlan
{
    @Override
    public String name()
    {
        return "test";
    }

    @Override
    public Operator plan()
    {
        // select id, value from test
        GroupTable groupTable = ais().getGroup("test").getGroupTable();
        UserTable testTable = ais().getUserTable("test", "test");
        RowType testRowType = schema().userTableRowType(testTable);
        return
            project_Default(
                groupScan_Default(groupTable),
                testRowType,
                Arrays.asList(Expressions.field(testRowType, 0),
                              Expressions.field(testRowType, 1)));
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final int[] TYPES = new int[]{Types.INTEGER, Types.VARCHAR};
}
