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

package com.akiban.server.test.mt.mthapi.base;

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
