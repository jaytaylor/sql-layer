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

package com.foundationdb.qp.util;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.qp.rowtype.Schema;

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
