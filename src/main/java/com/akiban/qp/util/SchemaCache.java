
package com.akiban.qp.util;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.qp.rowtype.Schema;

public final class SchemaCache {

    // static SchemaCache interface

    public static Schema globalSchema(AkibanInformationSchema ais) {
        return ais.getCachedValue(CACHE_KEY, CACHE_GENERATOR);
    }

    // class state

    private static final Object CACHE_KEY = new Object();
    private static final CacheValueGenerator<Schema> CACHE_GENERATOR = new CacheValueGenerator<Schema>() {
        @Override
        public Schema valueFor(AkibanInformationSchema ais) {
            return new Schema(ais);
        }
    };
}
