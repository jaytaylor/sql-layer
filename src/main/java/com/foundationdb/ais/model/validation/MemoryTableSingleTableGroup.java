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
import com.foundationdb.ais.model.Join;
import com.foundationdb.server.error.GroupMultipleMemoryTables;

/**
 * Validate the current assumption of groups with a memory table contain only one 
 * table, that is, there is no muti-table groups with the memory tables. 
 * (The MemoryTablesNotMixed validation ensures Memory tables are not mixed with other 
 *  types of tables). 
 *  TODO: It would be nice to remove this limitation of the current system. 
 * @author tjoneslo
 *
 */
public class MemoryTableSingleTableGroup implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            if (join.getChild().hasMemoryTableFactory()) {
                output.reportFailure(new AISValidationFailure (
                        new GroupMultipleMemoryTables(join.getParent().getName(), join.getChild().getName())));
            }
        }
    }

}
