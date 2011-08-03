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
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Visitor;
import com.akiban.server.error.ErrorCode;

/**
 * Validates the internal references used by the AIS are correct. 
 * @author tjoneslo
 *
 */
class ReferencesCorrect implements AISValidation,Visitor {

    private AISValidationOutput output = null;
    private Table visitingTable = null;
    private Index visitingIndex = null;
    private Group visitingGroup = null;
    
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        this.output = output;
        
        ais.traversePreOrder(this);
    }
    
    @Override
    public void visitUserTable(UserTable userTable) {
        visitingTable = userTable;
        if (userTable == null) {
            output.reportFailure(new AISValidationFailure(ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "null table found"));
        }
        if (userTable.isGroupTable()) {
            output.reportFailure(new AISValidationFailure(ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "wrong value for isGroupTable(): %s", userTable.getName().toString()));
        }
        
    }

    @Override
    public void visitColumn(Column column) {
        if (column == null) {
            output.reportFailure(new AISValidationFailure(ErrorCode.INTERNAL_REFERENCES_BROKEN, 
                    "null column in table %s", visitingTable.getName().toString()));
        }
        if (column.getTable() != visitingTable) {
            output.reportFailure(new AISValidationFailure(ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "column %s has bad reference to table %s", 
                    column.getName(), visitingTable.getName().toString()));
        }
    }

    @Override
    public void visitGroupTable(GroupTable groupTable) {
        visitingTable = groupTable;
        if (groupTable == null) {
            output.reportFailure(new AISValidationFailure(ErrorCode.INTERNAL_REFERENCES_BROKEN,
            "null table found"));
        }
        if (groupTable.isUserTable()) {
            output.reportFailure(new AISValidationFailure(ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "wrong value for isUserTable(): %s", groupTable.getName().toString()));
        }
    }

    @Override
    public void visitGroup(Group group) {
        visitingGroup = group;
        if (group == null) {
            output.reportFailure(new AISValidationFailure(ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "null group found"));
            return;
        }
        if (group.getGroupTable() == null) {
            output.reportFailure(new AISValidationFailure(ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "group %s has null group table", group.getName()));
        }
    }


    @Override
    public void visitIndex(Index index) {
        visitingIndex = index;
        if (index == null) {
            output.reportFailure(new AISValidationFailure (ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "null index in table %s", visitingTable.getName().toString()));
        } else if (index.isTableIndex() && index.rootMostTable() != visitingTable) {
            output.reportFailure(new AISValidationFailure (ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "Table index %s has bad reference to table %s",
                    index.getIndexName().toString(), visitingTable.getName().toString()));
        } else if (index.isGroupIndex() && ((GroupIndex)index).getGroup() != visitingGroup) {
            output.reportFailure(new AISValidationFailure (ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "Group index %s has bad reference to group %s",
                    index.getIndexName().toString(), 
                    visitingGroup.getName()));
        }
        
    }
    @Override
    public void visitIndexColumn(IndexColumn indexColumn) {
        if (indexColumn == null) {
            output.reportFailure(new AISValidationFailure (ErrorCode.INTERNAL_REFERENCES_BROKEN,
                    "null column in index %s", visitingIndex.getIndexName().toString()));
            return;
        }
        if (indexColumn.getIndex() != visitingIndex) {
            output.reportFailure(new AISValidationFailure (ErrorCode.INTERNAL_REFERENCES_BROKEN, 
                    "Index column has bad reference to index %s",
                    visitingIndex.getIndexName().toString()));
        }

    }

    @Override
    public void visitJoin(Join join) {}

    @Override
    public void visitJoinColumn(JoinColumn joinColumn) {}

    @Override
    public void visitType(Type type) {}

}
