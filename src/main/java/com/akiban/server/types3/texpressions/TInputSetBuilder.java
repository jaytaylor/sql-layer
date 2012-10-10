/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
        return new ArrayList<TInputSet>(inputSets);
    }

    private final InputSetFlags.Builder exactsBuilder = new InputSetFlags.Builder();
    private TInstanceNormalizer nextNormalizer;
    private boolean exact = false;
    private TInputSet vararg = null;

    private List<TInputSet> inputSets = new ArrayList<TInputSet>(4);
}
