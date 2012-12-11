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

package com.akiban.ais.model.validation;

import java.util.Collection;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AISValidationResults {
    private static final Logger LOG = LoggerFactory.getLogger(AISValidationResults.class);

    /**
     * Gets all failures, if there were any.
     *
     * The collection will be unmodifiable and immutable; if it is not empty, subsequent invocations of this method
     * may all return same collection instance, but only as long as no additional failures are reported. If new
     * failures are reported, they and the previous will be in a new collection instance.
     * @return an unmodifiable, immutable collection of failures; will be empty if all validations passed.
     */
    public Collection<AISValidationFailure> failures() {
        return Collections.unmodifiableCollection(failureList);
    }
    /**
     * Throws an InvalidOperationException if failures() returns a non-null collection.
     */
    public void throwIfNecessary() {
        if (!failureList.isEmpty()) {
            for (AISValidationFailure fail : failureList) {
                LOG.debug(String.format("Validation failure %s : %s", fail.errorCode(), fail.message()));
            }
            AISValidationFailure fail = failureList.iterator().next();
            fail.generateException();
        }
    }
 
    /**
     * Package-private constructor
     */
    protected  AISValidationResults() {}
    protected Collection<AISValidationFailure> failureList;
}
