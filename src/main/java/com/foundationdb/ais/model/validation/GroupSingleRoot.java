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
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.GroupHasMultipleRootsException;

class GroupSingleRoot implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Group group : ais.getGroups().values()) {
            validateGroup (ais, group, output);
        }
    }
    
    private void validateGroup (AkibanInformationSchema ais, Group group, AISValidationOutput output) {
        Table root = null;
        for (Table table : ais.getTables().values()) {
            if (table.getGroup() == group && table.isRoot()) {
                if (root == null) {
                    root = table;
                } else {
                    output.reportFailure(new AISValidationFailure (
                            new GroupHasMultipleRootsException(group.getName(), root.getName(), table.getName())));
                }
            }
        }
    }
}
