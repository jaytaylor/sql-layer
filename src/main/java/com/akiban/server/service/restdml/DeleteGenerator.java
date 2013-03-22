/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.server.service.restdml;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;

public class DeleteGenerator extends OperatorGenerator {

    public DeleteGenerator (AkibanInformationSchema ais) {
        super(ais);
    }
    
    @Override
    protected Operator create(TableName tableName) {
        Operator lookup = indexAncestorLookup(tableName); 
        // build delete operator.
        return API.delete_Returning(lookup, true, true);
    }       
    
}
