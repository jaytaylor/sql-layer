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

import java.util.Iterator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.error.JoinColumnMismatchException;
import com.foundationdb.server.error.JoinParentNoExplicitPK;
import com.foundationdb.server.error.JoinToWrongColumnsException;

class JoinToParentPK implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            
            // bug 931258: If parent has no external PK, flag this as an error. 
            if (join.getParent().getPrimaryKey() == null) {
                output.reportFailure(new AISValidationFailure(
                        new JoinParentNoExplicitPK (join.getParent().getName())));
                continue;
            }
            TableIndex parentPK= join.getParent().getPrimaryKey().getIndex();
            if (parentPK.getKeyColumns().size() != join.getJoinColumns().size()) {
                output.reportFailure(new AISValidationFailure(
                        new JoinColumnMismatchException (join.getJoinColumns().size(),
                                join.getChild().getName(),
                                join.getParent().getName(),
                                parentPK.getKeyColumns().size())));

                continue;
            }
            Iterator<JoinColumn>  joinColumns = join.getJoinColumns().iterator();
            for (IndexColumn parentPKColumn : parentPK.getKeyColumns()) {
                JoinColumn joinColumn = joinColumns.next();
                if (parentPKColumn.getColumn() != joinColumn.getParent()) {
                    output.reportFailure(new AISValidationFailure (
                            new JoinToWrongColumnsException (
                                    join.getChild().getName(), 
                                    joinColumn.getParent().getName(), 
                                    parentPK.getTable().getName(), parentPKColumn.getColumn().getName())));
                }
            }
        }
    }
}
