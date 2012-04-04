/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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

    public static ConditionExpression compare(ConstantExpression value, Comparison comparison, ColumnExpression column) {
        return new ComparisonCondition(comparison, value, column, null, null);
    }

    public static ConditionExpression isNull(ColumnExpression column) {
        return new FunctionCondition("isNull", Collections.<ExpressionNode>singletonList(column), null, null);
    }

    public static ConditionExpression or(ConditionExpression left, ConditionExpression right) {
        return new LogicalFunctionCondition("or", Arrays.asList(left, right), null, null);
    }

    public static ConditionExpression and(ConditionExpression left, ConditionExpression right) {
        return new LogicalFunctionCondition("and", Arrays.asList(left, right), null, null);
    }

    public static ConditionExpression not(ConditionExpression expression) {
        return new LogicalFunctionCondition("not", Arrays.asList(expression), null, null);
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
        TableSource source = new TableSource(node, true, "t1");
        lastName = new ColumnExpression(source, table.getColumn("first_name"));
        firstName = new ColumnExpression(source, table.getColumn("last_name"));
    }

    private TUtils() {}
}
