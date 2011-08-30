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

package com.akiban.qp.physicaloperator;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.AggregatedRowType;
import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.Strings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class AggregateOperatorTest {

    @Test
    public void simpleNoGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG)
                .row(1L)
                .row(2L)
                .row(3L)
        );
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType());
        PhysicalOperator plan = new AggregationOperator(input, 0, FACTORY, TestFactory.FUNC_NAMES, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.VARCHAR)
                .row("1, 2, 3")
                .rows();
        check(plan, expected);
    }

    @Test
    public void simpleWithGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.LONG)
                .row(1L, 10L)
                .row(1L, 11L)
                .row(2L, 12L)
                .row(3L, 13L)
        );
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType());
        PhysicalOperator plan = new AggregationOperator(input, 1, FACTORY, TestFactory.FUNC_NAMES, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
                .row(1L, "10, 11")
                .row(2L, "12")
                .row(3L, "13")
                .rows();
        check(plan, expected);
    }

    @Test
    public void partiallyOrderedGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.LONG)
                .row(1L, 10L)
                .row(2L, 11L)
                .row(3L, 12L)
                .row(1L, 13L)
                .row(1L, 14L)
        );
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType());
        PhysicalOperator plan = new AggregationOperator(input, 1, FACTORY, TestFactory.FUNC_NAMES, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
                .row(1L, "10")
                .row(2L, "11")
                .row(3L, "12")
                .row(1L, "13, 14")
                .rows();
        check(plan, expected);
    }

    private static void check(PhysicalOperator plan, Deque<Row> expecteds) {
        List<Row> actuals = execute(plan);
        assertEquals("size (expecteds=" + expecteds+", actuals=" + actuals + ')', expecteds.size(), actuals.size());
        int rowCount = 0;
        try {
            for (Row actual : actuals) {
                Row expected = expecteds.remove();
                for (int i = 0; i < plan.rowType().nFields(); ++i) {
                    ValueHolder actualHolder = new ValueHolder(actual.bindSource(i, UndefBindings.only()));
                    ValueHolder expectedHolder = new ValueHolder(expected.bindSource(i, UndefBindings.only()));

                    if (!expectedHolder.equals(actualHolder)) {
                        assertEquals(
                                String.format("row[%d] field[%d]", rowCount, i),
                                str(expecteds),
                                str(actuals)
                        );
                        assertEquals(String.format("row[%d] field[%d]", rowCount, i), expectedHolder, actualHolder);
                        throw new AssertionError("should have failed by now!");
                    }
                }
                ++rowCount;
            }
        }
        finally {
            for (Row actual : actuals) {
                actual.release();
            }
        }
    }

    private static String str(Collection<? extends Row> rows) {
        return Strings.join(rows);
    }

    private static List<Row> execute(PhysicalOperator plan) {
        List<Row> rows = new ArrayList<Row>();
        Cursor cursor = plan.cursor(null);
        cursor.open(UndefBindings.only());
        try {
            for(Row row = cursor.next(); row != null; row = cursor.next()) {
                row.share();
                rows.add(row);
            }
            return rows;
        } finally {
            cursor.close();
        }
    }

    // consts

    private static AggregatorFactory FACTORY = new TestFactory();

    // nested classes

    private static class TestFactory implements AggregatorFactory {
        @Override
        public Aggregator get(String name) {
            assert name.equals(FUNC_NAME);
            return new TestAggregator();
        }

        @Override
        public void validateNames(List<String> names) {
            assert FUNC_NAMES.containsAll(names);
        }


        private static final String FUNC_NAME = "MAIN";
        private static final List<String> FUNC_NAMES = Collections.singletonList(FUNC_NAME);
    }

    private static class TestAggregator implements Aggregator {

        @Override
        public AkType outputType() {
            return AkType.VARCHAR;
        }

        @Override
        public void input(ValueSource input) {
            long value = Extractors.getLongExtractor(AkType.LONG).getLong(input);
            result.append(value).append(", ");
        }

        @Override
        public void output(ValueTarget output) {
            final String asString;
            if (result.length() == 0) {
                asString = "";
            }
            else {
                result.setLength(result.length() - 2);
                asString = result.toString();
            }
            result.setLength(0);
            Converters.convert(new ValueHolder(AkType.VARCHAR, asString), output);
        }

        private StringBuilder result = new StringBuilder();
    }
}
