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

package com.akiban.server.service.memcache.outputter.jsonoutputter;

import java.util.Queue;

public interface Genealogist<T>
{
    /**
     * If x is an ancestor of y then objects h1, ..., hn are added to missing such that:
     * - x is the parent of h1,
     * - h1 is the parent of h2,
     * ...
     * - h(n-1) is the parent of hn, and
     * - hn is the parent of y.
     * If x is a parent of y, or is not an ancestor of y, then missing is not modified.
     * The first time fillInMissing is called for a sequence of objects, x will be null,
     * and y will be the first element of the sequence.
     * @param x An object in a hierarchy, or null on the first invocation for a sequence.
     * @param y An object in a hierarchy, never null.
     * @param missingRows records filled in descendents of x.
     */
    void fillInMissing(T x, T y, Queue<T> missingRows);
}
