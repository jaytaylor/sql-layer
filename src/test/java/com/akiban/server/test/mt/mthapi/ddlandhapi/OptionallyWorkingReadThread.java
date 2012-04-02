/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
