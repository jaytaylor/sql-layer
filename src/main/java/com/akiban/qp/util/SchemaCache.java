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

public final class SchemaCache {

    // static SchemaCache interface

    public static Schema globalSchema(AkibanInformationSchema ais) {
        return GLOBAL.schema(ais);
    }

    // SchemaCache interface

    private Schema schema(AkibanInformationSchema ais) {
        synchronized(LOCK) {
            if (this.ais != ais) {
                this.schema = new Schema(ais);
            }
            return schema;
        }
    }

    // object state

    private final Object LOCK = new Object();
    private AkibanInformationSchema ais = null;
    private Schema schema = null;

    // class state

    private static final SchemaCache GLOBAL = new SchemaCache();
}
