
package com.akiban.server.types3;

import com.akiban.server.types3.texpressions.TValidatedOverload;

public interface TInstanceNormalizer {
    void apply(TInstanceAdjuster adapter, TValidatedOverload overload, TInputSet inputSet, int max);
}
