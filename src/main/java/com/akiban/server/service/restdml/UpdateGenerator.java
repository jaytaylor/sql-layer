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
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Operator;
import com.akiban.server.service.restdml.OperatorGenerator.RowStream;

public class UpdateGenerator extends OperatorGenerator {

    private UserTable table;

    public UpdateGenerator(AkibanInformationSchema ais) {
        super(ais);
    }

    @Override
    protected Operator create(TableName tableName) {
        table = ais().getUserTable(tableName);

        RowStream stream = assembleValueScan (table);
        //stream = assembleProjectTable (stream, table);
        
        return null;
    }

}
