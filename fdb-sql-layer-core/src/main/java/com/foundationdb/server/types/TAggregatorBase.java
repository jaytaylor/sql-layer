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

import com.foundationdb.util.BitSets;
import com.google.common.base.Predicate;

import java.util.Collections;
import java.util.List;

public abstract class TAggregatorBase implements TAggregator {

    @Override
    public String id() {
        return getClass().getName();
    }

    @Override
    public int[] getPriorities() {
        return new int[] { 1 };
    }

    @Override
    public String[] registeredNames() {
        return new String[] { displayName() };
    }

    @Override
    public final String displayName() {
        return name;
    }

    @Override
    public List<TInputSet> inputSets() {
        return Collections.singletonList(
                new TInputSet(inputClass, BitSets.of(0), false, inputClass == null, null));
    }

    @Override
    public InputSetFlags exactInputs() {
        return InputSetFlags.ALL_OFF;
    }

    @Override
    public final String toString() {
        return displayName();
    }

    @Override
    public Predicate<List<? extends TPreptimeValue>> isCandidate() {
        return null;
    }

    protected TClass inputClass() {
        return inputClass;
    }

    protected TAggregatorBase(String name, TClass inputClass) {
        this.name = name;
        this.inputClass = inputClass;
    }

    private final String name;
    private final TClass inputClass;
}
