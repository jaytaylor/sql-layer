/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
