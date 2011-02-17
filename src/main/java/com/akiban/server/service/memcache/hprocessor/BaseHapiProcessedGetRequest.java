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

package com.akiban.server.service.memcache.hprocessor;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiProcessedGetRequest;

import java.util.List;

abstract class BaseHapiProcessedGetRequest implements HapiProcessedGetRequest {
    private final HapiGetRequest request;

    protected BaseHapiProcessedGetRequest(HapiGetRequest request) {
        this.request = request;
    }

    @Override
    public String getSchema() {
        return request.getSchema();
    }

    @Override
    public String getTable() {
        return request.getTable();
    }

    @Override
    public TableName getUsingTable() {
        return request.getUsingTable();
    }

    @Override
    public List<HapiPredicate> getPredicates() {
        return request.getPredicates();
    }
}
