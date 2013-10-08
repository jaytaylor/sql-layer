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

package com.foundationdb.server.types;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A very thin shim around List<TClass></TClass>. Mostly there so that the call sites don't have to worry about
 * generics. This is especially useful for the reflective registration, where it's easier to search for a TCastPath
 * than fora {@code Collection&lt;? extends List&lt;? extends TClass&gt;&gt;}.
 */
public final class TCastPath {

    public static TCastPath create(TClass first, TClass second, TClass third, TClass... rest) {
        TClass[] all = new TClass[rest.length + 3];
        all[0] = first;
        all[1] = second;
        all[2] = third;
        System.arraycopy(rest, 0, all, 3, rest.length);
        List<? extends TClass> list = Arrays.asList(all);
        return new TCastPath(list);
    }

    private TCastPath(List<? extends TClass> list) {
        if (list.size() < 3)
            throw new IllegalArgumentException("cast paths must contain at least three elements: " + list);
        this.list = Collections.unmodifiableList(list);
    }

    public List<? extends TClass> getPath() {
        return list;
    }

    private final List<? extends TClass> list;
}
