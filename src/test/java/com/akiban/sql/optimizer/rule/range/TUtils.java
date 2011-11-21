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

package com.akiban.sql.optimizer.rule.range;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConditionExpression;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.FunctionCondition;
import com.akiban.sql.optimizer.plan.LogicalFunctionCondition;
import com.akiban.sql.optimizer.plan.TableNode;
import com.akiban.sql.optimizer.plan.TableSource;
import com.akiban.sql.optimizer.plan.TableTree;

import java.util.Arrays;
import java.util.Collections;

final class TUtils {

    public static ConstantExpression constant(String value) {
        return new ConstantExpression(value, AkType.VARCHAR);
    }

    public static ConstantExpression constant(long value) {
        return new ConstantExpression(value, AkType.LONG);
    }

    public static ConditionExpression compare(ColumnExpression column, Comparison comparison, ConstantExpression value) {
        return new ComparisonCondition(comparison, column, value, null, null);
    }

    public static ConditionExpression isNull(ColumnExpression column) {
        return new FunctionCondition("isNull", Collections.<ExpressionNode>singletonList(column), null, null);
    }

    public static ExpressionNode or(ConditionExpression left, ConditionExpression right) {
        return new LogicalFunctionCondition("or", Arrays.asList(left, right), null, null);
    }

    public static ExpressionNode and(ConditionExpression left, ConditionExpression right) {
        return new LogicalFunctionCondition("and", Arrays.asList(left, right), null, null);
    }

    public static ConditionExpression sin(ColumnExpression column) {
        return new FunctionCondition("sin", Collections.<ExpressionNode>singletonList(column), null, null);
    }

    public static RangeSegment segment(RangeEndpoint start, RangeEndpoint end) {
        return new RangeSegment(start, end);
    }

    public static RangeEndpoint inclusive(long value) {
        return RangeEndpoint.inclusive(constant(value));
    }

    public static RangeEndpoint exclusive(long value) {
        return RangeEndpoint.exclusive(constant(value));
    }

    public static RangeEndpoint inclusive(String value) {
        return RangeEndpoint.inclusive(constant(value));
    }

    public static RangeEndpoint exclusive(String value) {
        return RangeEndpoint.exclusive(constant(value));
    }

    public static final ColumnExpression lastName;
    public static final ColumnExpression firstName;

    static {
        AkibanInformationSchema ais = AISBBasedBuilder.create("s")
            .userTable("t1").colString("first_name", 32).colString("last_name", 32)
            .ais();
        UserTable table = ais.getUserTable("s", "t1");
        TableNode node = new TableNode(table, new TableTree());
        TableSource source = new TableSource(node, true);
        lastName = new ColumnExpression(source, table.getColumn("first_name"));
        firstName = new ColumnExpression(source, table.getColumn("last_name"));
    }

    private TUtils() {}
}
