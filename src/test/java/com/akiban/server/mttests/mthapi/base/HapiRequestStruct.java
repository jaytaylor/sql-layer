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

package com.akiban.server.mttests.mthapi.base;

import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;

public final class HapiRequestStruct {
    private final HapiGetRequest request;
    private final SaisTable selectRoot;
    private final SaisTable predicatesTable;

    public HapiRequestStruct(HapiGetRequest request, SaisTable selectRoot) {
        this(request, selectRoot, selectRoot);
    }

    public HapiRequestStruct(HapiGetRequest request, SaisTable selectRoot, SaisTable predicatesTable) {
        this.request = request;
        this.selectRoot = selectRoot;
        this.predicatesTable = predicatesTable;
    }

    public HapiGetRequest getRequest() {
        return request;
    }

    public SaisTable getSelectRoot() {
        return selectRoot;
    }

    public SaisTable getPredicatesTable() {
        return predicatesTable;
    }
}
