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

package com.akiban.server.test.mt.mthapi.ddlandhapi;

import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.test.mt.mthapi.base.HapiRequestStruct;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import com.akiban.server.test.mt.mthapi.common.BasicHapiSuccess;
import com.akiban.server.test.mt.mthapi.common.HapiValidationError;
import com.akiban.util.ThreadlessRandom;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Collections;
import java.util.EnumSet;

abstract class OptionallyWorkingReadThread extends BasicHapiSuccess {
    private final EnumSet<HapiRequestException.ReasonCode> validErrors;
    private final float chance;
    private final String rootKey;

    protected OptionallyWorkingReadThread(String schema, SaisTable root, float chance,
                                          HapiRequestException.ReasonCode... validErrors)
    {
        this(schema, root, chance, true, validErrors);
    }
    protected OptionallyWorkingReadThread(String schema, SaisTable root, float chance, boolean requireSingleColPKs,
                                          HapiRequestException.ReasonCode... validErrors)
    {
        super(schema, root, requireSingleColPKs);
        if (chance < 0 || chance > 1) {
            throw new IllegalArgumentException(Float.toString(chance));
        }
        this.validErrors = EnumSet.noneOf(HapiRequestException.ReasonCode.class);
        Collections.addAll(this.validErrors, validErrors);
        this.chance = chance;
        this.rootKey = '@' + root.getName();
    }

    @Override
    protected void validateErrorResponse(HapiGetRequest request, Throwable exception)
            throws UnexpectedException
    {
        if (exception instanceof HapiRequestException) {
            HapiRequestException hre = (HapiRequestException) exception;
            if (validErrors.contains(hre.getReasonCode())) {
                return;
            }
        }
        super.validateErrorResponse(request, exception);
    }

    @Override
    protected void validateSuccessResponse(HapiRequestStruct requestStruct, JSONObject result) throws JSONException {
        super.validateSuccessResponse(requestStruct, result);
        HapiValidationError.assertFalse(HapiValidationError.Reason.ROOT_TABLES_COUNT,
                "more than one root found",
                result.getJSONArray(rootKey).length() > 1);
        // Also, we must have results!
//                TODO: this isn't a valid test while we allow concurrent scans and adding/dropping of indexes
//                see: https://answers.launchpad.net/akiban-server/+question/148857
//                HapiValidationError.assertEquals(HapiValidationError.Reason.ROOT_TABLES_COUNT,
//                        "number of roots",
//                        1, result.getJSONArray("@p").length()
//                );
    }

    @Override
    protected abstract HapiRequestStruct pullRequest(ThreadlessRandom random);

    @Override
    protected int spawnCount() {
        float spawnRoughly = chance * super.spawnCount();
        return (int)(spawnRoughly + .5);
    }
}
