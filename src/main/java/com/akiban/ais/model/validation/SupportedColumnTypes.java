
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
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
