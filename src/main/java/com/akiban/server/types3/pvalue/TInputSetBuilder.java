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

package com.akiban.server.types3.pvalue;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInputSet;
import com.akiban.util.BitSets;

import java.util.ArrayList;
import java.util.List;

public final class TInputSetBuilder {
    
    public TInputSetBuilder covers(TClass targetType, int... covering) {
        inputSets.add(new TInputSet(targetType, BitSets.of(covering), false));
        return this;
    }
    
    public TInputSetBuilder vararg(TClass targetType, int... covering) {
        inputSets.add(new TInputSet(targetType, BitSets.of(covering), true));
        return this;
    }

    List<TInputSet> toList() {
        return new ArrayList<TInputSet>(inputSets);
    }

    private List<TInputSet> inputSets = new ArrayList<TInputSet>(4);
}
