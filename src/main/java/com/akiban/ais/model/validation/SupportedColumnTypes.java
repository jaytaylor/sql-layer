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
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Visitor;
import com.akiban.server.error.UnsupportedDataTypeException;
import com.akiban.server.error.UnsupportedIndexDataTypeException;


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
            failures.reportFailure(new AISValidationFailure (
                    new UnsupportedDataTypeException (column.getTable().getName(),
                            column.getName(), column.getType().name())));
        }
    }

    @Override
    public void visitIndexColumn(IndexColumn indexColumn) {
        if (!sourceAIS.isTypeSupportedAsIndex(indexColumn.getColumn().getType().name())) {
            failures.reportFailure(new AISValidationFailure (
                    new UnsupportedIndexDataTypeException (
                            new TableName (indexColumn.getIndex().getIndexName().getSchemaName(), 
                            indexColumn.getIndex().getIndexName().getTableName()),
                            indexColumn.getIndex().getIndexName().getName(), 
                            indexColumn.getColumn().getName(),
                            indexColumn.getColumn().getType().name())));
        }
    }
    @Override
    public void visitGroup(Group group) {}
    @Override
    public void visitGroupTable(GroupTable groupTable) {}
    @Override
    public void visitIndex(Index index) {}
    @Override
    public void visitJoin(Join join) {}
    @Override
    public void visitJoinColumn(JoinColumn joinColumn) {}
    @Override
    public void visitType(Type type) {}
    @Override
    public void visitUserTable(UserTable userTable) {}
}
