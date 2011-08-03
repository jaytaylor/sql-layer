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
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ErrorCode;

/**
 * This validates a current limitation of the system which expects all of the 
 * UserTable indexes are also reflected in the group table. 
 */
class TableIndexesMatchGroupIndexes implements AISValidation {

    NameGenerator names = new DefaultNameGenerator();
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            if (table.getGroup() == null) { continue; }
            for (TableIndex index : table.getIndexesIncludingInternal()) {
                if (table.getGroup().getGroupTable().getIndex(names.generateGroupIndexName(index)) == null) {
                    output.reportFailure(new AISValidationFailure (ErrorCode.VALIDATION_FAILURE, 
                            "User index %s not reflected in group table index list",
                            index.getIndexName().toString()));
                }
            }
        }
    }
}
