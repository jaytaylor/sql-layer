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

package com.akiban.cserver.service.memcache.hprocessor;

import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.common.NoSuchTableException;

import java.util.Collections;
import java.util.Set;

final class DummyProcessedRequest extends BaseHapiProcessedGetRequest {
    DummyProcessedRequest(HapiGetRequest request) {
        super(request);
    }

    @Override
    public Set<String> getProjectedTables() {
        return Collections.emptySet();
    }

    @Override
    public RowDef getRowDef(int tableId) {
        throw new UnsupportedOperationException();
    }
}
