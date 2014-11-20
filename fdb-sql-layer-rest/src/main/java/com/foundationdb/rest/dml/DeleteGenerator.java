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
package com.foundationdb.rest.dml;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;

public class DeleteGenerator extends OperatorGenerator {

    public DeleteGenerator (AkibanInformationSchema ais) {
        super(ais);
    }
    
    protected Operator create(TableName tableName) {
        Operator lookup = indexAncestorLookup(tableName); 
        // build delete operator.
        return API.delete_Returning(lookup, true);
    }       
}
