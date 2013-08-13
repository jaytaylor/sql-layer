/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

import java.util.Map;

public final class MapDiff {
    
    public static <K,V> void apply(
            Map<? extends K, ? extends V> origMap,
            Map<? extends K, ? extends V> updatedMap,
            MapDiffHandler<? super K, ? super V> handler)
    {
        for (Map.Entry<? extends K, ? extends V> origEntry : origMap.entrySet()) {
            K key = origEntry.getKey();
            if (updatedMap.containsKey(key))
                handler.inBoth(key, origEntry.getValue(), updatedMap.get(key));
            else
                handler.dropped(origEntry.getValue());
        }
        for (Map.Entry<? extends K, ? extends V> updatedEntry : updatedMap.entrySet()) {
            if (!origMap.containsKey(updatedEntry.getKey()))
                handler.added(updatedEntry.getValue());
        }
    }
    
    private MapDiff() {}
}
