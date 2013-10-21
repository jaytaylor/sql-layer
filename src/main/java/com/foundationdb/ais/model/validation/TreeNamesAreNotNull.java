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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.error.GroupTreeNameIsNullException;
import com.foundationdb.server.error.IndexTreeNameIsNullException;
import com.foundationdb.server.error.SequenceTreeNameIsNullException;

import java.util.Collection;

/**
 * Check all group and index tree names are not null.
 */
public class TreeNamesAreNotNull implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(Table table : ais.getTables().values()) {
            checkTable(table);
        }
        for(Group group : ais.getGroups().values()) {
            checkGroup(group);
            for(Index index : group.getIndexes()) {
                checkIndex(index);
            }
        }
        for (Sequence sequence: ais.getSequences().values()) {
            checkSequence(sequence);
        }
    }

    private static void checkGroup(Group group) {
        if(group.getTreeName() == null) {
            throw new GroupTreeNameIsNullException(group);
        }
    }

    private static void checkTable(Table table) {
        for(Index index : table.getIndexesIncludingInternal()) {
            checkIndex(index);
        }
    }

    private static void checkIndex(Index index) {
        if(index.getTreeName() == null) {
            throw new IndexTreeNameIsNullException(index);
        }
    }
    
    private static void checkSequence(Sequence sequence) {
        if (sequence.getTreeName() == null) {
            throw new SequenceTreeNameIsNullException(sequence);
        }
    }
}
