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
package com.akiban.ais.model.validation;

import java.util.Collection;
import java.util.Collections;

import com.akiban.message.ErrorCode;
import com.akiban.server.InvalidOperationException;

public abstract class AISValidationResults {
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
            String firstFailure = failureList.iterator().next().message(); 
            throw new InvalidOperationException(ErrorCode.VALIDATION_FAILURE, firstFailure);
        }
    }
 
    /**
     * Package-private constructor
     */
    protected  AISValidationResults() {}
    protected Collection<AISValidationFailure> failureList;
}