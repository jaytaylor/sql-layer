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

package com.akiban.server.test.mt.mthapi.base;

import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;

public final class HapiRequestStruct {
    private final HapiGetRequest request;
    private final SaisTable selectRoot;
    private final SaisTable predicatesTable;
    private final String expectedIndex;

    public HapiRequestStruct(HapiGetRequest request, SaisTable selectRoot, String expectedIndex) {
        this(request, selectRoot, selectRoot, expectedIndex);
    }

    public HapiRequestStruct(HapiGetRequest request, SaisTable selectRoot, SaisTable predicatesTable,
                             String expectedIndex)
    {
        this.request = request;
        this.selectRoot = selectRoot;
        this.predicatesTable = predicatesTable;
        this.expectedIndex = expectedIndex;
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

    public boolean expectedIndexKnown() {
        return expectedIndex != null;
    }

    public String getExpectedIndex() {
        return expectedIndex;
    }
}
