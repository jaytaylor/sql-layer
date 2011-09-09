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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.server.error.UnsupportedUniqueGroupIndexException;

public class GroupIndexesNotUnique implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Group group : ais.getGroups().values()) {
            for (Index index : group.getIndexes()) {
                if (index.isUnique()) {
                    output.reportFailure(new AISValidationFailure (
                            new UnsupportedUniqueGroupIndexException(index.getIndexName().getName())));
                }
            }
        }
    }
}
