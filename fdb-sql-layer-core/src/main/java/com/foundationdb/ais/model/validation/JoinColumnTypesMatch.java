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
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.server.error.JoinColumnTypesMismatchException;
import com.foundationdb.server.types.common.types.TypeValidator;

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
            if (join.getParent().getPrimaryKey() == null) {
                //bug 931258: Attempting to join to a table without a explicit PK,
                // causes getJoinColumns to throw an JoinParentNoExplicitPK exception. 
                // This is explicitly validated in JoinToParentPK
                continue;
            }
            for (JoinColumn column : join.getJoinColumns()) {
                Column parentCol = column.getParent();
                Column childCol = column.getChild();
                if (!TypeValidator.isSupportedForJoin(parentCol.getType(), childCol.getType())) {
                    output.reportFailure(new AISValidationFailure (
                            new JoinColumnTypesMismatchException (parentCol.getTable().getName(), parentCol.getName(),
                                    childCol.getTable().getName(), childCol.getName())));
                }
            }
        }
    }
}
