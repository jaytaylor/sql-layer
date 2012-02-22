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

import java.util.Iterator;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.server.error.JoinColumnMismatchException;
import com.akiban.server.error.JoinToWrongColumnsException;

class JoinToParentPK implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            // this is caught by TableHasPrimaryKey validation
            if (join.getParent().getPrimaryKey() == null) { continue; }
            TableIndex parentPK = join.getParent().getPrimaryKey().getIndex();
            
            if (parentPK.getKeyColumns().size() != join.getJoinColumns().size()) {
                output.reportFailure(new AISValidationFailure(
                        new JoinColumnMismatchException (join.getJoinColumns().size(),
                                join.getChild().getName(),
                                join.getParent().getName(), 
                                parentPK.getKeyColumns().size())));
                        
                return;
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
