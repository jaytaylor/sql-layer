/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.server.error.DuplicateGroupTreeNamesException;

import java.util.HashMap;
import java.util.Map;

class GroupTreeNamesUnique implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Map<String,Group> treeNameMap = new HashMap<>();

        for(Group group : ais.getGroups().values()) {
            String treeName = group.getTreeName();
            Group curGroup = treeNameMap.put(treeName, group);
            if(curGroup != null) {
                UserTable root = group.getRoot();
                UserTable curRoot = curGroup.getRoot();
                output.reportFailure(
                    new AISValidationFailure(
                            new DuplicateGroupTreeNamesException(root.getName(), curRoot.getName(), treeName)));
            }
        }
    }
}
