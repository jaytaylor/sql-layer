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

import java.util.HashSet;
import java.util.Set;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;


/**
 * Validates each table has either zero or one join to a parent.  
 * @author tjoneslo
 *
 */
public class JoinToOneParent implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Set<UserTable> childTables = new HashSet<UserTable>();
        
        for (Join join : ais.getJoins().values()) {
            if (childTables.contains(join.getChild())) {
                output.reportFailure(new AISValidationFailure (ErrorCode.DUPLICATE_TABLE,
                        "Table %s has joins to two parents",
                        join.getChild().getName().toString()));
            } else {
                childTables.add(join.getChild());
            }
        }
    }

}
