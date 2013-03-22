
package com.akiban.server.types3.texpressions;

import com.akiban.server.types3.InputSetFlags;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstanceNormalizer;
import com.akiban.util.BitSets;

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

    List<TInputSet> toList() {
        return new ArrayList<>(inputSets);
    }

    private final InputSetFlags.Builder exactsBuilder = new InputSetFlags.Builder();
    private TInstanceNormalizer nextNormalizer;
    private boolean exact = false;
    private TInputSet vararg = null;

    private List<TInputSet> inputSets = new ArrayList<>(4);
}
