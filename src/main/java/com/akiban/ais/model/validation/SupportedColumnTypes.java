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
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Visitor;
import com.akiban.message.ErrorCode;

class SupportedColumnTypes implements AISValidation, Visitor {

    private AkibanInformationSchema sourceAIS;
    private AISValidationOutput     failures;
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        sourceAIS = ais;
        failures = output;
        ais.traversePreOrder(this);
    }
    @Override
    public void visitColumn(Column column) {
        if (!sourceAIS.isTypeSupported(column.getType().name())) {
            failures.reportFailure(new AISValidationFailure (ErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Table %s has column %s with unsupported data type %s",
                    column.getTable().getName().toString(),
                    column.getName(), column.getType().name()));
        }
    }

    @Override
    public void visitIndexColumn(IndexColumn indexColumn) {
        if (!sourceAIS.isTypeSupportedAsIndex(indexColumn.getColumn().getType().name())) {
            failures.reportFailure(new AISValidationFailure (ErrorCode.UNSUPPORTED_INDEX_DATA_TYPE,
                    "Index %s has column %s with unspported index data type %s",
                    indexColumn.getIndex().getIndexName().toString(), 
                    indexColumn.getColumn().getName(), 
                    indexColumn.getColumn().getType().name()));
        }
            
    }
    @Override
    public void visitGroup(Group group) {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void visitGroupTable(GroupTable groupTable) {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void visitIndex(Index index) {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void visitJoin(Join join) {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void visitJoinColumn(JoinColumn joinColumn) {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void visitType(Type type) {
        // TODO Auto-generated method stub
        
    }
    @Override
    public void visitUserTable(UserTable userTable) {
        // TODO Auto-generated method stub
        
    }
}
