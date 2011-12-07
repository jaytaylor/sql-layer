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
import com.akiban.ais.model.UserTable;

import com.akiban.server.error.DuplicateIndexIdException;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

class IndexIDsUnique implements AISValidation {

    private Map<Group, Map<Integer, Index>> byGroup;
    private AISValidationOutput failures; 
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        byGroup = new HashMap<Group, Map<Integer, Index>>();
        this.failures = output;
        
        for (UserTable table : ais.getUserTables().values()) {
            for (Index index : table.getIndexes()) {
                checkIndexID(table.getGroup(), index);
            }
        }
        for (Group group : ais.getGroups().values()) {
            for (Index index : group.getIndexes()) {
                checkIndexID(group, index);
            }
        }
    }
    
    private void checkIndexID(Group group, Index index) {
        Map<Integer, Index> forGroup = byGroup.get(group);
        if (forGroup == null) {
            forGroup = new TreeMap<Integer, Index>();
            byGroup.put(group, forGroup);
        }
        Index other = forGroup.put(index.getIndexId(), index);
        if (other != null) {
            failures.reportFailure(new AISValidationFailure(new DuplicateIndexIdException(other.getIndexName(), index.getIndexName())));
        }
    }
}
