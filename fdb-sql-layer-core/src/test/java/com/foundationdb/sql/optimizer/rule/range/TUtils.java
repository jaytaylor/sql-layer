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

package com.foundationdb.sql.optimizer.rule.range;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ComparisonCondition;
import com.foundationdb.sql.optimizer.plan.ConditionExpression;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.FunctionCondition;
import com.foundationdb.sql.optimizer.plan.LogicalFunctionCondition;
import com.foundationdb.sql.optimizer.plan.TableNode;
import com.foundationdb.sql.optimizer.plan.TableSource;
import com.foundationdb.sql.optimizer.plan.TableTree;

import java.util.Arrays;
import java.util.Collections;

final class TUtils {

    public static ConstantExpression constant(String value) {
        return new ConstantExpression(value, MString.VARCHAR.instance(true));
    }

    public static ConstantExpression constant(long value) {
        return new ConstantExpression(value, MNumeric.BIGINT.instance(true));
    }

    public static ConditionExpression compare(ColumnExpression column, Comparison comparison, ConstantExpression value) {
        return new ComparisonCondition(comparison, column, value, null, null, null);
    }

    public static ConditionExpression compare(ConstantExpression value, Comparison comparison, ColumnExpression column) {
        return new ComparisonCondition(comparison, value, column, null, null, null);
    }

    public static ConditionExpression isNull(ColumnExpression column) {
        return new FunctionCondition("isNull", Collections.<ExpressionNode>singletonList(column), null, null, null);
    }

    public static ConditionExpression or(ConditionExpression left, ConditionExpression right) {
        return new LogicalFunctionCondition("or", Arrays.asList(left, right), null, null, null);
    }

    public static ConditionExpression and(ConditionExpression left, ConditionExpression right) {
        return new LogicalFunctionCondition("and", Arrays.asList(left, right), null, null, null);
    }

    public static ConditionExpression not(ConditionExpression expression) {
        return new LogicalFunctionCondition("not", Arrays.asList(expression), null, null, null);
    }

    public static ConditionExpression sin(ColumnExpression column) {
        return new FunctionCondition("sin", Collections.<ExpressionNode>singletonList(column), null, null, null);
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

    public static RangeEndpoint nullInclusive(String value) {
        return RangeEndpoint.nullInclusive(constant(value));
    }

    public static RangeEndpoint nullExclusive(String value) {
        return RangeEndpoint.nullExclusive(constant(value));
    }

    public static final ColumnExpression lastName;
    public static final ColumnExpression firstName;

    static {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        AkibanInformationSchema ais = AISBBasedBuilder.create("s", typesTranslator)
            .table("t1").colString("first_name", 32).colString("last_name", 32)
            .ais();
        Table table = ais.getTable("s", "t1");
        TableNode node = new TableNode(table, new TableTree());
        TableSource source = new TableSource(node, true, "t1");
        lastName = new ColumnExpression(source, table.getColumn("first_name"));
        firstName = new ColumnExpression(source, table.getColumn("last_name"));
    }

    private TUtils() {}
}
