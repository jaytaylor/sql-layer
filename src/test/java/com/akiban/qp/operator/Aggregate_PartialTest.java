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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.AggregatedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.Strings;
import com.akiban.util.WrappingByteSource;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class Aggregate_PartialTest {

    @Test
    public void simpleNoGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG)
                .row(1L)
                .row(2L)
                .row(3L)
        );
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 0, aggregators);
        Operator plan = new Aggregate_Partial(input, input.rowType(), 0, aggregators, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.VARCHAR)
                .row("1, 2, 3")
                .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void simpleWithGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.LONG)
                .row(1L, 10L)
                .row(1L, 11L)
                .row(2L, 12L)
                .row(3L, 13L)
        );
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 1, aggregators);
        Operator plan = new Aggregate_Partial(input, input.rowType(), 1, aggregators, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
                .row(1L, "10, 11")
                .row(2L, "12")
                .row(3L, "13")
                .rows();
        OperatorTestHelper.check(plan, expected);
    }

    /**
     * Situation like <tt>SELECT name FROM customers GROUP BY name</tt>. In this case, all of the columns are
     * GROUP BY, and there's no aggregator.
     */
    @Test
    public void groupByWithoutAggregators() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG)
                .row(1L)
                .row(1L)
                .row(2L)
                .row(3L)
                .row(1L)
        );
        List<AggregatorFactory> aggregators = Collections.<AggregatorFactory>emptyList();
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 1, aggregators);
        Operator plan = new Aggregate_Partial(input, input.rowType(), 1, aggregators, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.LONG)
                .row(1L)
                .row(2L)
                .row(3L)
                .row(1L)
                .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void partiallyOrderedGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.LONG)
                .row(null, 8L)
                .row(null, 9L)
                .row(1L, 10L)
                .row(2L, 11L)
                .row(3L, 12L)
                .row(null, 13L)
                .row(null, 14L)
                .row(1L, 15L)
                .row(1L, 16L)
        );
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 1, aggregators);
        Operator plan = new Aggregate_Partial(input, input.rowType(), 1, aggregators, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
                .row(null, "8, 9")
                .row(1L, "10")
                .row(2L, "11")
                .row(3L, "12")
                .row(null, "13, 14")
                .row(1L, "15, 16")
                .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void twoColumnGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.LONG)
                .row(1L, "alpha", 1)
                .row(1L, "alpha", 2)
                .row(1L, "bravo", 3)
                .row(2L, "bravo", 4)
                .row(2L, "charlie", 5)
        );
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 2, aggregators);
        Operator plan = new Aggregate_Partial(input, input.rowType(), 2, aggregators, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.VARCHAR)
                .row(1L, "alpha", "1, 2")
                .row(1L, "bravo", "3")
                .row(2L, "bravo", "4")
                .row(2L, "charlie", "5")
                .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void noInputRowsNoGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG));
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 0, aggregators);
        Operator plan = new Aggregate_Partial(input, input.rowType(), 0, aggregators, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.VARCHAR)
                .row(new ValueHolder(AkType.VARCHAR, EMPTY))
                .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void noInputRowsWithGroupBy() {
        TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.LONG));
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 1, aggregators);
        Operator plan = new Aggregate_Partial(input, input.rowType(), 1, aggregators, rowType);
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR).rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void uninterestingRowsPassThrough() {
        RowsBuilder boringRows = new RowsBuilder(AkType.VARCHAR, AkType.LONG)
                .row("A", 1L)
                .row("B", 2L)
                .row("C", 3L);
        RowsBuilder interestingRows = new RowsBuilder(AkType.LONG, AkType.LONG)
                .row(10L, 100L)
                .row(10L, 101L)
                .row(20L, 200L);
        // Shuffle rows into this order
        // (10, 100)
        // ("A", 1)
        // (10, 101)
        // ("B", 2)
        // (20, 200)
        // ("C", 3)
        // We'll expect the output in this order:
        // ("A", 1)
        // ("B", 2)
        // (10, "100, 101")
        // ("C", 3)
        // (20, "200")
        Deque<Row> shuffled = shuffle(interestingRows.rows(), boringRows.rows(), new ArrayDeque<Row>());
        // now in this order:

        TestOperator input = new TestOperator(shuffled, interestingRows.rowType());
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 1, aggregators);
        Operator plan = new Aggregate_Partial(input, input.rowType(), 1, aggregators, rowType);

        // Create the expected output, including rows that have passed through
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
                .row(10L, "100, 101")
                .row(20L, "200")
                .rows();
        List<Row> expectedFull = new ArrayList<Row>();
        expectedFull.addAll(boringRows.rows());
        expectedFull.addAll(expected);
        Collections.swap(expectedFull, 2, 3);

        // Finally, check
        OperatorTestHelper.check(plan, expectedFull);
    }

    @Test(expected = InconvertibleTypesException.class)
    public void invalidRowTypes() {
        Operator plan;
        try {
            TestOperator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARBINARY)
                    .row(wrapLong(1L), wrapBytes(0x01, 0x02))
            );
            List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
            AggregatedRowType rowType = new AggregatedRowType(null, 1, input.rowType(), 1, aggregators);
            plan = new Aggregate_Partial(input, input.rowType(), 1, aggregators, rowType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        OperatorTestHelper.execute(plan);
    }

    @Test(expected = NullPointerException.class)
    public void inputIsNull() {
        TestOperator input;
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType;
        try {
            input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARBINARY)
                    .row(wrapLong(1L), wrapBytes(0x01, 0x02))
            );
            rowType = new AggregatedRowType(null, 1, input.rowType(), 1, aggregators);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        new Aggregate_Partial(null, input.rowType(), 1, aggregators, rowType);
    }

    @Test(expected = NullPointerException.class)
    public void factoriesListIsNull() {
        TestOperator input;
        AggregatedRowType rowType;
        try {
            input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARBINARY)
                    .row(wrapLong(1L), wrapBytes(0x01, 0x02))
            );
            rowType = new AggregatedRowType(null, 1, input.rowType(), 1, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        new Aggregate_Partial(input, input.rowType(), 1, null, rowType);
    }

    @Test(expected = NullPointerException.class)
    public void rowTypeIsNull() {
        TestOperator input;
        try {
            input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARBINARY)
                    .row(wrapLong(1L), wrapBytes(0x01, 0x02))
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        new Aggregate_Partial(input, input.rowType(), 1, Collections.singletonList(TEST_AGGREGATOR), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void inputsIndexTooLow() {
        TestOperator input;
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType;
        try {
            input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARBINARY)
                    .row(wrapLong(1L), wrapBytes(0x01, 0x02))
            );
            rowType = new AggregatedRowType(null, 1, input.rowType(), -1, aggregators);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        new Aggregate_Partial(input, input.rowType(), -1, aggregators, rowType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void inputsIndexTooHigh() {
        TestOperator input;
        List<AggregatorFactory> aggregators = Collections.singletonList(TEST_AGGREGATOR);
        AggregatedRowType rowType;
        try {
            input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARBINARY)
                    .row(wrapLong(1L), wrapBytes(0x01, 0x02))
            );
            rowType = new AggregatedRowType(null, 1, input.rowType(), 2, aggregators);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        new Aggregate_Partial(input, input.rowType(), 2, aggregators, rowType);
    }

    private static <R extends Row,C extends Collection<? super Row>>
    C shuffle(Collection<? extends R> first, Collection<? extends R> second, C output)
    {
        Iterator<? extends Row> secondIter = second.iterator();
        for (Row fromFirst : first) {
            output.add(fromFirst);
            if (secondIter.hasNext()) {
                output.add(secondIter.next());
            }
        }
        while (secondIter.hasNext()) {
            output.add(secondIter.next());
        }
        assert output.size() == first.size() + second.size()
                : String.format("%d != %d + %d: %s from %s %s",
                    output.size(), first.size(), second.size(), output, first, second
        );
        return output;
    }

    private ValueHolder wrapBytes(int... bytes) {
        byte[] asBytes = new byte[bytes.length];
        for (int i=0; i < bytes.length; ++i) {
            byte asByte = (byte)bytes[i];
            asBytes[i] = asByte;
        }
        return new ValueHolder(AkType.VARBINARY, new WrappingByteSource(asBytes));
    }

    private ValueHolder wrapLong(long value) {
        return new ValueHolder(AkType.LONG, value);
    }

    // const

    private static final String EMPTY = "empty";
    private static final AggregatorFactory TEST_AGGREGATOR = new AggregatorFactory() {
        @Override
        public Aggregator get() {
            return new TestAggregator();
        }
        @Override
        public AkType outputType() {
            return AkType.VARCHAR;
        }
    };

    // nested classes

    private static class TestAggregator implements Aggregator {

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

        @Override
        public ValueSource emptyValue() {
            return new ValueHolder(AkType.VARCHAR, EMPTY);
        }

        private StringBuilder result = new StringBuilder();
    }
}
