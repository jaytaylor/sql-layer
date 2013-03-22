
package com.akiban.server.store.statistics.histograms;

import java.util.Arrays;
import java.util.List;

final class BucketTestUtils {
    public static <T> Bucket<T> bucket(T value, long equals, long lessThans, long lessThanDistincts) {
        Bucket<T> result = new Bucket<>();
        result.init(value, 1);
        for (int i = 1; i < equals; ++i) { // starting count from 1 since a new Bucket has equals of 1
            result.addEquals();
        }
        result.addLessThans(lessThans);
        result.addLessThanDistincts(lessThanDistincts);
        return result;
    }

    public static class SingletonSplitter<T> implements Splitter<T> {
        @Override
        public int segments() {
            return 1;
        }

        @Override
        public List<? extends T> split(T input) {
            oneElementList.set(0, input);
            return oneElementList;
        }

        @SuppressWarnings("unchecked")
        private List<T> oneElementList = Arrays.asList((T[])new Object[1]);
    }
}
