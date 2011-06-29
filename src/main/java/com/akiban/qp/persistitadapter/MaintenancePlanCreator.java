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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.util.CachePair;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class MaintenancePlanCreator
        implements CachePair.CachedValueProvider<AkibanInformationSchema, OperatorStoreMaintenancePlans>
{

    // CachedValueProvider interface

    @Override
    public OperatorStoreMaintenancePlans valueFor(AkibanInformationSchema ais) {
        Schema schema = SchemaCache.globalSchema(ais);
        return new OperatorStoreMaintenancePlans(schema, ais.getGroups().values());
    }

    // for use by this package (in production)

    // package consts

    static final int HKEY_BINDING_POSITION = 0;
}
