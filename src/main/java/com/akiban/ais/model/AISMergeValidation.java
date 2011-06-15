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
package com.akiban.ais.model;

import com.akiban.server.InvalidOperationException;

/**
 * AIS Merge Validation verifies correctness of one element of the validateSchema before
 * it is merged into the targetSchema. If the validation fails, the validate method throws
 * the InvalidOperationException. 
 * 
 * The AISMerge creates a collection of the AISMergeValidations, and runs each in turn 
 * on the merge validation process. 
 * 
 * @author tjoneslo
 *
 */
public interface AISMergeValidation {
    public void validate (AkibanInformationSchema targetSchema, AkibanInformationSchema validateSchema) 
        throws InvalidOperationException;
}
