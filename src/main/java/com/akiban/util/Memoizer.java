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

import com.google.common.base.Function;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public abstract class Memoizer<I,O> implements Function<I,O> {
    protected abstract O compute(I input);
    
    public O set(I input, O value) {
        return results.put(input, value);
    }

    @Override
    public O apply(@Nullable I input) {
        return get(input);
    }

    public O get(I input) {
        if (results.containsKey(input))
            return results.get(input);
        O result = compute(input);
        O old = results.put(input, result);
        assert old == null : "old value not null, implying concurrent access: " + old;
        return result;
    }
    
    private Map<I,O> results = new HashMap<I, O>();
}
