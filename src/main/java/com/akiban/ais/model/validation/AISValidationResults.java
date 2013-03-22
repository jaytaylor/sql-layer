
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
