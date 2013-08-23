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

package com.foundationdb.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public final class ListUtils {

    /**
     * Ensures that there are no more than {@code size} elements in the given {@code list}. If the list already has
     * {@code size} elements or fewer, this doesn't do anything; otherwise, it'll remove enough elements from the
     * list to ensure that {@code list.size() == size}. Either way, by the end of this invocation,
     * {@code list.size() <= size}.
     * @param list the incoming list.
     * @param size the maximum number of elements to keep in {@code list}
     * @throws IllegalArgumentException if {@code size < 0}
     * @throws NullPointerException if {@code list} is {@code null}
     * @throws UnsupportedOperationException if the list doesn't support removal
     */
    public static void truncate(List<?> list, int size) {
        ArgumentValidation.isGTE("truncate size", size, 0);

        int rowsToRemove = list.size() - size;
        if (rowsToRemove <= 0) {
            return;
        }
        ListIterator<?> iterator = list.listIterator(list.size());
        while (rowsToRemove-- > 0) {
            iterator.previous();
            iterator.remove();
        }
    }

    public static void removeDuplicates(List<?> list) {
        Set<Object> elems = new HashSet<>(list.size());
        for(Iterator<?> iter = list.iterator(); iter.hasNext();) {
            Object next = iter.next();
            if (!elems.add(next))
                iter.remove();
        }
    }
}
