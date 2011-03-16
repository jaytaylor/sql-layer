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

import com.akiban.ais.model.Index;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.util.ThreadlessRandom;
import org.json.JSONObject;

public abstract class HapiReadThread {
    private static final int DEFAULT_SPAWN_COUNT = 1000;

    public static class UnexpectedException extends Exception {
        public UnexpectedException(HapiGetRequest request, Throwable cause) {
            super(String.format("%s caused unexpected exception", request), cause);
        }
    }

    public static class UnexpectedSuccess extends Exception {
        public UnexpectedSuccess(HapiGetRequest request) {
            super(String.format("%s should not have succeeded", request));
        }
    }

    protected abstract HapiRequestStruct pullRequest(ThreadlessRandom random);

    protected int spawnCount() {
        return DEFAULT_SPAWN_COUNT;
    }

    protected abstract void validateIndex(HapiRequestStruct request, Index queriedIndex);

    protected abstract void validateSuccessResponse(HapiRequestStruct request, JSONObject result) throws Exception;
    protected abstract void validateErrorResponse(HapiGetRequest request, Throwable exception) throws Exception;

}
