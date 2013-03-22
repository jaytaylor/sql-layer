
package com.akiban.ais.model;

public interface CacheValueGenerator<V> {
    V valueFor(AkibanInformationSchema ais);
}
