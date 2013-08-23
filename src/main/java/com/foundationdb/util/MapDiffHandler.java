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

public interface MapDiffHandler<K,V> {
    void added(V element);
    void dropped(V element);
    void inBoth(K key, V original, V updated);

    public static class Default<K,V> implements MapDiffHandler<K, V> {
        @Override
        public void added(V element) {}

        @Override
        public void dropped(V element) {}

        @Override
        public void inBoth(K key, V original, V updated) {}
    }
}
