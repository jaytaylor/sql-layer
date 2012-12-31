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

package com.akiban.server.types3.common.types;

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TParser;
import com.akiban.server.types3.*;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.sql.types.TypeId;

public class NoAttrTClass extends SimpleDtdTClass {

    @Override
    public TInstance instance(boolean nullable) {
        TInstance result;
        // These two blocks are obviously racy. However, the race will not create incorrectness, it'll just
        // allow there to be multiple copies of the TInstance floating around, each of which is correct, immutable
        // and equivalent.
        if (nullable) {
            result = nullableTInstance;
            if (result == null) {
                result = createInstanceNoArgs(true);
                nullableTInstance = result;
            }
        }
        else {
            result = notNullableTInstance;
            if (result == null) {
                result = createInstanceNoArgs(false);
                notNullableTInstance = result;
            }
        }
        return result;
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        return right; // doesn't matter which!
    }

    @Override
    protected void validate(TInstance instance) {
    }

    public NoAttrTClass(TBundleID bundle, String name, Enum<?> category, TClassFormatter formatter, int internalRepVersion,
                           int serializationVersion, int serializationSize, PUnderlying pUnderlying, TParser parser,
                           int defaultVarcharLen, TypeId typeId) {
        super(bundle, name, category, formatter, Attribute.NONE.class, internalRepVersion, serializationVersion, serializationSize,
                pUnderlying, parser, defaultVarcharLen, typeId);
    }

    private volatile TInstance nullableTInstance;
    private volatile TInstance notNullableTInstance;
}
