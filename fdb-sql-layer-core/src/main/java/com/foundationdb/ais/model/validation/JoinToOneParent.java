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

import java.util.HashSet;
import java.util.Set;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.JoinToMultipleParentsException;


/**
 * Validates each table has either zero or one join to a parent.  
 * @author tjoneslo
 *
 */
public class JoinToOneParent implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        Set<Table> childTables = new HashSet<>();
        
        for (Join join : ais.getJoins().values()) {
            if (childTables.contains(join.getChild())) {
                output.reportFailure(new AISValidationFailure (
                        new JoinToMultipleParentsException (join.getChild().getName())));
            } else {
                childTables.add(join.getChild());
            }
        }
    }

}
