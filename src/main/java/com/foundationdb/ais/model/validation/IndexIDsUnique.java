/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.ais.model.validation;

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;

import com.foundationdb.server.error.DuplicateIndexIdException;

import java.util.Map;
import java.util.HashMap;

public class IndexIDsUnique implements AISValidation
{
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        ais.visit(new IndexIDVisitor(output));
    }

    private static class IndexIDVisitor extends AbstractVisitor
    {
        private final Map<Integer,Index> current = new HashMap<>();
        private final AISValidationOutput failures;

        private IndexIDVisitor(AISValidationOutput failures) {
            this.failures = failures;
        }

        @Override
        public void visit(Group group) {
            current.clear();
        }

        @Override
        public void visit(Index index) {
            Index prev = current.put(index.getIndexId(), index);
            if(prev != null) {
                failures.reportFailure(
                    new AISValidationFailure(new DuplicateIndexIdException(prev.getIndexName(), index.getIndexName()))
                );
            }
        }
    }
}
