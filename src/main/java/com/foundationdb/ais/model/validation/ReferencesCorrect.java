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
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.Visitor;
import com.foundationdb.server.error.AISNullReferenceException;
import com.foundationdb.server.error.BadAISReferenceException;

/**
 * Validates the internal references used by the AIS are correct. 
 * @author tjoneslo
 *
 */
class ReferencesCorrect implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        ReferenceVisitor visitor = new ReferenceVisitor(output);
        ais.visit(visitor);
    }

    private static class ReferenceVisitor implements Visitor {
        private final AISValidationOutput output;
        private Table visitingTable = null;
        private Index visitingIndex = null;
        private Group visitingGroup = null;

        public ReferenceVisitor(AISValidationOutput output) {
            this.output = output;
        }

        @Override
        public void visit(Group group) {
            visitingGroup = group;
            if (group == null) {
                output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException("ais", "", "group")));
            } else if (group.getRoot() == null) {
                output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException("group", group.getName().toString(), "root table")));
            }
        }

        @Override
        public void visit(Table table) {
            visitingTable = table;
            if (table == null) {
                output.reportFailure(new AISValidationFailure(
                        new AISNullReferenceException ("ais", "", "user table")));
            }
        }

        @Override
        public void visit(Column column) {
            if (column == null) {
                output.reportFailure(new AISValidationFailure(
                        new AISNullReferenceException ("user table", visitingTable.getName().toString(), "column")));
            } else if (column.getTable() != visitingTable) {
                output.reportFailure(new AISValidationFailure(
                        new BadAISReferenceException ("column", column.getName(), "table", visitingTable.getName().toString())));
            }
        }

        @Override
        public void visit(Index index) {
            visitingIndex = index;
            if (index == null) {
                output.reportFailure(new AISValidationFailure (
                        new AISNullReferenceException ("table", visitingTable.getName().toString(), "index")));
            } else if (index.isTableIndex() && index.rootMostTable() != visitingTable) {
                output.reportFailure(new AISValidationFailure (
                        new BadAISReferenceException ("Table index", index.getIndexName().toString(),
                                                      "table", visitingTable.getName().toString())));
            } else if (index.isGroupIndex() && ((GroupIndex)index).getGroup() != visitingGroup) {
                output.reportFailure(new AISValidationFailure (
                        new BadAISReferenceException ("Group index", index.getIndexName().toString(),
                                                      "group", visitingGroup.getName().toString())));
            }
        }

        @Override
        public void visit(IndexColumn indexColumn) {
            if (indexColumn == null) {
                output.reportFailure(new AISValidationFailure (
                        new AISNullReferenceException ("index", visitingIndex.getIndexName().toString(), "column")));
            } else if (indexColumn.getIndex() != visitingIndex) {
                output.reportFailure(new AISValidationFailure (
                        new BadAISReferenceException ("Index column",indexColumn.getColumn().getName(),
                                                      "index", visitingIndex.getIndexName().toString())));
            }
        }
    }
}
