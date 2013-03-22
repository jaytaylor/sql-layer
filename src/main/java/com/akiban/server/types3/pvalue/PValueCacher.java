
package com.akiban.server.types3.pvalue;

import com.akiban.server.types3.TInstance;

public interface PValueCacher {
    void cacheToValue(Object cached, TInstance tInstance, PBasicValueTarget target);
    Object valueToCache(PBasicValueSource value, TInstance tInstance);
    Object sanitize(Object object);
    boolean canConvertToValue(Object cached);
}
