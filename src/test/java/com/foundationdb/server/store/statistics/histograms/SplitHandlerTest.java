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

package com.foundationdb.server.store.statistics.histograms;

import com.foundationdb.util.AssertUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class SplitHandlerTest {
    @Test
    public void singleStream() {
        List<String> inputs = Arrays.asList(
                "one",
                "one",
                "two",
                "three",
                "three",
                "three"
        );
        List<SplitPair> onlyStream = Arrays.asList(
                new SplitPair("one", 2),
                new SplitPair("two", 1),
                new SplitPair("three", 3)
        );

        List<List<SplitPair>> expected = Collections.singletonList(onlyStream);
        List<List<SplitPair>> actual = run(new BucketTestUtils.SingletonSplitter<String>(), inputs);
        AssertUtils.assertCollectionEquals("single stream", expected, actual);
    }

    @Test
    public void twoStreamsSameLength() {
        List<String> inputs = Arrays.asList(
                "one a",
                "one a",
                "two b",
                "three c",
                "three c",
                "three c"
        );
        List<SplitPair> firstStream = Arrays.asList(
                new SplitPair("one", 2),
                new SplitPair("two", 1),
                new SplitPair("three", 3)
        );
        List<SplitPair> secondStream = Arrays.asList(
                new SplitPair("a", 2),
                new SplitPair("b", 1),
                new SplitPair("c", 3)
        );
        
        Splitter<String> splitter = new Splitter<String>() {
            @Override
            public int segments() {
                return 2;
            }

            @Override
            public List<? extends String> split(String input) {
                return Arrays.asList(input.split(" "));
            }
        };

        List<List<SplitPair>> expected = new ArrayList<>();
        expected.add(firstStream);
        expected.add(secondStream);

        List<List<SplitPair>> actual = run(splitter, inputs);
        AssertUtils.assertCollectionEquals("single stream", expected, actual);
    }

    @Test
    public void twoStreamsDifferentLength() {
        List<String> inputs = Arrays.asList(
                "one a",
                "one a",
                "two a",
                "three c",
                "three c",
                "three c"
        );
        List<SplitPair> firstStream = Arrays.asList(
                new SplitPair("one", 2),
                new SplitPair("two", 1),
                new SplitPair("three", 3)
        );
        List<SplitPair> secondStream = Arrays.asList(
                new SplitPair("a", 3),
                new SplitPair("c", 3)
        );

        Splitter<String> splitter = new Splitter<String>() {
            @Override
            public int segments() {
                return 2;
            }

            @Override
            public List<? extends String> split(String input) {
                return Arrays.asList(input.split(" "));
            }
        };

        List<List<SplitPair>> expected = new ArrayList<>();
        expected.add(firstStream);
        expected.add(secondStream);


        List<List<SplitPair>> actual = run(splitter, inputs);
        AssertUtils.assertCollectionEquals("single stream", expected, actual);
    }
    
    @Test
    public void noInputs() {
        ToArraySplitHandler handler = new ToArraySplitHandler(new BucketTestUtils.SingletonSplitter<String>());
        handler.init();
        handler.finish();
        List<List<SplitPair>> actual = handler.splitPairStreams;
        List<SplitPair> emptyStream = Collections.emptyList();
        assertEquals("empty input results", Collections.singletonList(emptyStream), actual);
    }

    @Test(expected = IllegalStateException.class)
    public void visitBeforeInit() {
        ToArraySplitHandler handler = new ToArraySplitHandler(new BucketTestUtils.SingletonSplitter<String>());
        handler.visit("foo");
    }

    @Test(expected = IllegalStateException.class)
    public void finishBeforeInit() {
        ToArraySplitHandler handler = new ToArraySplitHandler(new BucketTestUtils.SingletonSplitter<String>());
        handler.finish();
    }

    @Test(expected = IllegalStateException.class)
    public void mismatchedSegmentsCount() {
        Splitter<String> splitter = new Splitter<String>() {
            @Override
            public int segments() {
                return 1; // lie! we'll be returning a list of size 2
            }

            @Override
            public List<? extends String> split(String input) {
                return Arrays.asList(input.split(" "));
            }
        };
        ToArraySplitHandler handler = new ToArraySplitHandler(splitter);
        handler.init();
        handler.visit("one two");
    }

    private List<List<SplitPair>> run(Splitter<String> splitter, List<String> inputs) {
        ToArraySplitHandler handler = new ToArraySplitHandler(splitter);
        handler.init();
        for (String input : inputs) {
            handler.visit(input);
        }
        handler.finish();

        return handler.splitPairStreams;
    }
    
    private static class ToArraySplitHandler extends SplitHandler<String> {
        @Override
        protected void handle(int segmentIndex, String input, int count) {
            SplitPair splitPair = new SplitPair(input, count);
            List<SplitPair> stream = splitPairStreams.get(segmentIndex);
            stream.add(splitPair);
        }

        private ToArraySplitHandler(Splitter<String> objectSplitter) {
            super(objectSplitter);
            int segments = objectSplitter.segments();
            splitPairStreams = new ArrayList<>(segments);
            for (int i = 0; i < segments; ++i) {
                splitPairStreams.add(new ArrayList<SplitPair>());
            }
        }

        final List<List<SplitPair>> splitPairStreams;
    }
    
    private static class SplitPair {
        
        private SplitPair(Object value, int count) {
            this.value = value;
            this.count = count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SplitPair splitPair = (SplitPair) o;
            return count == splitPair.count && value.equals(splitPair.value);

        }

        @Override
        public int hashCode() {
            int result = value.hashCode();
            result = 31 * result + count;
            return result;
        }

        @Override
        public String toString() {
            return String.format("(%s)x%d", value, count);
        }

        final Object value;
        final int count;
    }
}
