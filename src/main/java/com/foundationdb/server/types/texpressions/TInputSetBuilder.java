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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.types.InputSetFlags;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstanceNormalizer;
import com.foundationdb.util.BitSets;

import java.util.ArrayList;
import java.util.List;

public final class TInputSetBuilder {

    public TInputSetBuilder covers(TClass targetType, int... covering) {
        inputSets.add(new TInputSet(targetType, BitSets.of(covering), false, false, nextNormalizer));
        nextNormalizer = null;
        setExacts(covering);
        return this;
    }

    public TInputSetBuilder pickingCovers(TClass targetType, int... covering) {
        inputSets.add(new TInputSet(targetType, BitSets.of(covering), false, true, nextNormalizer));
        nextNormalizer = null;
        return this;
    }

    public TInputSetBuilder vararg(TClass targetType, int... covering) {
        assert vararg == null : vararg;
        vararg = new TInputSet(targetType, BitSets.of(covering), true, false, nextNormalizer);
        nextNormalizer = null;
        inputSets.add(vararg);
        exactsBuilder.setVararg(exact);
        return this;
    }

    public TInputSetBuilder pickingVararg(TClass targetType, int... covering) {
        assert vararg == null : vararg;
        vararg = new TInputSet(targetType, BitSets.of(covering), true, true, nextNormalizer);
        inputSets.add(vararg);
        nextNormalizer = null;
        exactsBuilder.setVararg(exact);
        return this;
    }

    public TInputSetBuilder reset(TInputSetBuilder builder) {
        inputSets = builder.toList();
        return this;
    }

    public TInputSetBuilder setExact(boolean exact) {
        this.exact = exact;
        return this;
    }

    public TInputSetBuilder nextInputPicksWith(TInstanceNormalizer nextNormalizer) {
        this.nextNormalizer = nextNormalizer;
        return this;
    }

    public void setExact(int pos, boolean exact) {
        if (exact) {
            exactsBuilder.set(pos, true);
        }
    }

    private void setExacts(int[] positions) {
        if (exact) {
            for (int pos : positions) {
                setExact(pos, true);
            }
        }
    }

    public InputSetFlags exactInputs() {
        return exactsBuilder.get();
    }

    public List<TInputSet> toList() {
        return new ArrayList<>(inputSets);
    }

    private final InputSetFlags.Builder exactsBuilder = new InputSetFlags.Builder();
    private TInstanceNormalizer nextNormalizer;
    private boolean exact = false;
    private TInputSet vararg = null;

    private List<TInputSet> inputSets = new ArrayList<>(4);
}
