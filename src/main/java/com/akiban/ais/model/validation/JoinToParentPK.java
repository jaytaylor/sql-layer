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
import com.akiban.message.ErrorCode;

class JoinToParentPK implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            // this is caught by TableHasPrimaryKey validation
            if (join.getParent().getPrimaryKey() == null) { continue; }
            TableIndex parentPK = join.getParent().getPrimaryKey().getIndex();
            
            if (parentPK.getColumns().size() != join.getJoinColumns().size()) {
                output.reportFailure(new AISValidationFailure(ErrorCode.JOIN_TO_WRONG_COLUMNS,
                        "Join %s join column list size (%d) does not match parent table (%s) PK list size (%d)",
                        join.getName(), join.getJoinColumns().size(), 
                        join.getParent().getName().toString(), parentPK.getColumns().size()));
                return;
            }
            Iterator<JoinColumn>  joinColumns = join.getJoinColumns().iterator();            
            for (IndexColumn parentPKColumn : parentPK.getColumns()) {
                JoinColumn joinColumn = joinColumns.next();
                if (parentPKColumn.getColumn() != joinColumn.getParent()) {
                    output.reportFailure(new AISValidationFailure (ErrorCode.JOIN_TO_WRONG_COLUMNS,
                            "Join %s has mis-matched column (%s) to parent table PK column (%s)",
                            join.getName(), joinColumn.getParent().getName(), parentPKColumn.getColumn().getName()));
                }
            }
        }
    }
}
