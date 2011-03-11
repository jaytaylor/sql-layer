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
import org.json.JSONObject;

public abstract class HapiSuccess extends HapiReadThread {
    @Override
    protected abstract void validateSuccessResponse(HapiRequestStruct request, JSONObject result) throws Exception;

    @Override
    protected void validateErrorResponse(HapiGetRequest request, Throwable exception) throws UnexpectedException {
        throw new UnexpectedException(request, exception);
    }
}
