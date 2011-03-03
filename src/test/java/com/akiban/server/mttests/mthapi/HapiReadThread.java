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

package com.akiban.server.mttests.mthapi;

import com.akiban.server.api.HapiGetRequest;
import org.json.JSONObject;

abstract class HapiReadThread {
    private static final int DEFAULT_SPAWN_COUNT = 25;

    static class UnexpectedException extends Exception {
        public UnexpectedException(HapiGetRequest request, Throwable cause) {
            super(String.format("%s caused unexpected exception", request), cause);
        }
    }

    public static class UnexpectedSuccess extends Exception {
        public UnexpectedSuccess(HapiGetRequest request) {
            super(String.format("%s should not have succeeded", request));
        }
    }

    private HapiReadThread() {
        // private, since we want to maintan the invariant that only one validation method is overridden
    }

    protected abstract HapiGetRequest pullRequest();

    protected int spawnCount() {
        return DEFAULT_SPAWN_COUNT;
    }

    abstract void validateSuccessResponse(HapiGetRequest request, JSONObject result) throws UnexpectedSuccess;
    abstract void validateErrorResponse(HapiGetRequest request, Throwable exception) throws UnexpectedException;

    public abstract static class HapiSuccess extends HapiReadThread {
        @Override
        protected abstract void validateSuccessResponse(HapiGetRequest request, JSONObject result);

        @Override
        final void validateErrorResponse(HapiGetRequest request, Throwable exception) throws UnexpectedException {
            throw new UnexpectedException(request, exception);
        }
    }

    public abstract static class HapiError extends HapiReadThread {
        protected abstract void validateErrorResponse(HapiGetRequest request, Throwable exception);

        @Override
        final void validateSuccessResponse(HapiGetRequest request, JSONObject result) throws UnexpectedSuccess {
            throw new UnexpectedSuccess(request);
        }
    }
}
