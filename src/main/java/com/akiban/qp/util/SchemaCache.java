/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.util;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.rowtype.Schema;
import com.akiban.util.CachePair;

public final class SchemaCache {

    // static SchemaCache interface

    public static Schema globalSchema(AkibanInformationSchema ais) {
        return GLOBAL.get(ais);
    }

    // class state

    private static final CachePair<AkibanInformationSchema, Schema> GLOBAL = CachePair.using(
            new CachePair.CachedValueProvider<AkibanInformationSchema, Schema>() {
                @Override
                public Schema valueFor(AkibanInformationSchema ais) {
                    return new Schema(ais);
                }
            }
    );
}
