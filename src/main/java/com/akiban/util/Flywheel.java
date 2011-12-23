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
package com.akiban.util;

import java.util.ArrayDeque;
import java.util.Queue;

public abstract class Flywheel<T> implements Recycler<T> {

    @Override
    public String toString() {
        return String.format("%d in reserve, %d created",
                reserves == null ? 0 : reserves.size(),
                created
        );
    }

    protected abstract T createNew();
    
    protected int defaultCapacity() {
        return 16;
    }
    
    public T get() {
        if (reserves == null || reserves.isEmpty()) {
            ++created;
            return createNew();
        }
        return reserves.poll();
    }
    
    public void recycle(T element) {
        if (reserves == null)
            reserves = new ArrayDeque<T>(defaultCapacity());
        reserves.offer(element);
    }

    private Queue<T> reserves;
    private int created;
}
