
package com.akiban.server.types3;

import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.BitSet;

public final class TInputSet {

    public boolean isPicking() {
        return isPicking;
    }

    public TClass targetType() {
        return targetType;
    }

    public int positionsLength() {
        return covering.length();
    }

    public boolean covers(int index) {
        return covering.get(index);
    }

    public boolean coversRemaining() {
        return coversRemaining;
    }

    public int firstPosition() {
        return covering.nextSetBit(0);
    }

    public int nextPosition(int from) {
        return covering.nextSetBit(from);
    }

    public TInstanceNormalizer instanceAdjuster() {
        assert normalizer != null;
        return normalizer;
    }

    public TInputSet(TClass targetType, BitSet covering, boolean coversRemaining, boolean isPicking,
                     TInstanceNormalizer normalizer)
    {
        this.targetType = targetType;
        this.covering = covering.get(0, covering.length());
        this.coversRemaining = coversRemaining;
        this.isPicking = isPicking;
        if (normalizer != null)
            this.normalizer = normalizer;
        else if (targetType != null)
            this.normalizer = new PickingNormalizer(targetType);
        else
            this.normalizer = null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean coversAny = ! covering.isEmpty();
        if (coversAny) {
            sb.append("POS(");
            for (int i = covering.nextSetBit(0); i >= 0; i = covering.nextSetBit(i+1)) {
                sb.append(i).append(", ");
            }
            sb.setLength(sb.length() - 2); // trim trailing ", "
            sb.append(')');
        }
        if (coversRemaining) {
            if (coversAny)
                sb.append(", ");
            sb.append("REMAINING");
        }
        if (sb.length() == 0)
            sb.append("<none>"); // malformed input set, but still want a decent toString
        Object displayTargetType = (targetType == null) ? "*" : targetType;
        sb.append(" <- ").append(displayTargetType);
        return sb.toString();
    }

    private final TClass targetType;
    private final BitSet covering;
    private final boolean coversRemaining;
    private final boolean isPicking;
    private final TInstanceNormalizer normalizer;

    private static class PickingNormalizer implements TInstanceNormalizer {
        @Override
        public void apply(TInstanceAdjuster adjuster, TValidatedOverload overload, TInputSet inputSet, int max) {
            assert tclass != null : inputSet + " for " + overload;
            TInstance result = null;
            boolean resultEverChanged = false;
            for (int i = overload.firstInput(inputSet); i >= 0; i = overload.nextInput(inputSet, i+1, max)) {
                TInstance input = adjuster.get(i);
                if (result == null) {
                    result = input;
                }
                else {
                    TInstance picked = tclass.pickInstance(result, input);
                    resultEverChanged |= (!picked.equalsIncludingNullable(result));
                    result = picked;
                }
            }
            assert result != null : " no TInstance for " + inputSet + " in " + overload;
            if (resultEverChanged) {
                for (int i = overload.firstInput(inputSet); i >= 0; i = overload.nextInput(inputSet, i+1, max)) {
                    adjuster.replace(i, result);
                }
            }
        }

        private PickingNormalizer(TClass tclass) {
            this.tclass = tclass;
        }

        private final TClass tclass;
    }
}
