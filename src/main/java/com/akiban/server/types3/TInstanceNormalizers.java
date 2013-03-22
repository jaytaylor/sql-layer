
package com.akiban.server.types3;

import com.akiban.server.types3.texpressions.TValidatedOverload;

public final class TInstanceNormalizers {

    public static TInstanceNormalizer ALL_UNTOUCHED = new TInstanceNormalizer() {
        @Override
        public void apply(TInstanceAdjuster adapter, TValidatedOverload overload, TInputSet inputSet, int max) {}
    };

    private TInstanceNormalizers(){}
}
