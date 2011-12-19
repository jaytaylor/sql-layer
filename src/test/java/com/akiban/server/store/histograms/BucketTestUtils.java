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

package com.akiban.server.store.histograms;

import static com.akiban.util.CollectionUtils.list;

final class BucketTestUtils {
    public static <T> Bucket<T> bucket(T value, long equals, long lessThans, long lessThanDistincts) {
        Bucket<T> result = new Bucket<T>(value);
        for (int i = 1; i < equals; ++i) { // starting count from 1 since a new Bucket has equals of 1
            result.addEquals();
        }
        result.addLessThans(lessThans);
        result.addLessThanDistincts(lessThanDistincts);
        return result;
    }

    public static <T> Bucket<T> bucket(T value, long equals) {
        return bucket(value, equals, 0, 0);
    }

    public static <T> BucketSource<T> bucketSource(T... values) {
        return new BucketSource<T>(list(values));
    }
}
