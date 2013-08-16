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
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.ais.model.Visitor;
import com.foundationdb.server.error.UnsupportedDataTypeException;
import com.foundationdb.server.error.UnsupportedIndexDataTypeException;


class SupportedColumnTypes implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        ColumnTypeVisitor visitor = new ColumnTypeVisitor(output, ais);
        ais.traversePreOrder(visitor);
    }

    private static class ColumnTypeVisitor implements Visitor {
        private final AISValidationOutput failures;
        private final AkibanInformationSchema sourceAIS;

        private ColumnTypeVisitor(AISValidationOutput failures, AkibanInformationSchema sourceAIS) {
            this.failures = failures;
            this.sourceAIS = sourceAIS;
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
        public void visitGroup(Group group) {
        }

        @Override
        public void visitIndex(Index index) {
        }

        @Override
        public void visitJoin(Join join) {
        }

        @Override
        public void visitJoinColumn(JoinColumn joinColumn) {
        }

        @Override
        public void visitType(Type type) {
        }

        @Override
        public void visitUserTable(UserTable userTable) {
        }
    }
}
