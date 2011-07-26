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
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.message.ErrorCode;

/**
 * validate the columns used for the join in the parent (PK) and 
 * the child are join-able, according to the AIS.
 * @author tjoneslo
 *
 */
public class JoinColumnTypesMatch implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            for (JoinColumn column : join.getJoinColumns()) {
                Column parentCol = column.getParent();
                Column childCol = column.getChild();
                if(!ais.canTypesBeJoined(parentCol.getType().name(), childCol.getType().name())) {
                    output.reportFailure(new AISValidationFailure (ErrorCode.FK_TYPE_MISMATCH,
                            "Join %s has columns (parent %s and child %s) with different types (%s and %s)",
                            join.getName(), parentCol.getName(), childCol.getName(),
                            parentCol.getType().name(), childCol.getType().name()));
                }
            }
        }
    }
}
