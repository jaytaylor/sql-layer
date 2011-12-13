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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Visitor;
import com.akiban.server.error.JoinToProtectedTableException;
import com.akiban.server.error.ProtectedTableDDLException;

/**
 * Verifies the only tables in the akiban_information_schema are 
 * the ones on our fixed list of tables. 
 * @author tjoneslo
 *
 */
class ProtectedTables implements AISValidation,Visitor {

    private AISValidationOutput output = null;

    private static final Collection<String> PROTECT_LIST = createProtectList();
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        this.output = output;
        ais.traversePostOrder(this);
    }

    @Override
    public void visitUserTable(UserTable userTable) {
        if (userTable.getName().getSchemaName().equals(TableName.AKIBAN_INFORMATION_SCHEMA) &&
            !PROTECT_LIST.contains(userTable.getName().getTableName())) {
            output.reportFailure(new AISValidationFailure(
                    new ProtectedTableDDLException (userTable.getName())));
        }
    }

    @Override
    public void visitGroupTable(GroupTable groupTable) {
        if (groupTable.getName().getSchemaName().equals(TableName.AKIBAN_INFORMATION_SCHEMA) &&
                !PROTECT_LIST.contains(groupTable.getName().getTableName())) {
            output.reportFailure(new AISValidationFailure (
                    new ProtectedTableDDLException(groupTable.getName())));
        }
    }

    @Override
    public void visitJoin(Join join) {
        if (join.getParent().getName().getSchemaName().equals(TableName.AKIBAN_INFORMATION_SCHEMA) &&
            !PROTECT_LIST.contains(join.getParent().getName().getTableName())) {
            
            output.reportFailure(new AISValidationFailure (
                    new JoinToProtectedTableException (join.getChild().getName(), join.getParent().getName())));
        }
    }

    /**
     * TODO: This list needs to be coordinated with the real
     * list of tables in the akiban_information_schema. The
     * list here is temporary. 
     * @return
     */
    private static Collection<String> createProtectList() {
        
        NameGenerator names = new DefaultNameGenerator();
        LinkedList<String> list = new LinkedList<String>();
        list.add("groups");
        list.add(names.generateGroupTableName("groups"));
        list.add("tables");
        list.add(names.generateGroupTableName("tables"));
        list.add("columns");
        list.add(names.generateGroupTableName("columns"));
        list.add("joins");
        list.add(names.generateGroupTableName("joins"));
        list.add("join_columns");
        list.add(names.generateGroupTableName("join_columns"));
        list.add("indexes");
        list.add(names.generateGroupTableName("indexes"));
        list.add("index_columns");
        list.add(names.generateGroupTableName("index_columns"));
        list.add("types");
        list.add(names.generateGroupTableName("types"));
        list.add("index_analysis");
        list.add(names.generateGroupTableName("index_analysis"));
        list.add("index_statistics");
        list.add(names.generateGroupTableName("index_statistics"));
        list.add("index_statistics_entry");
        
        return Collections.unmodifiableList(list);
    }

    @Override
    public void visitColumn(Column column) {}

    @Override
    public void visitGroup(Group group) {}


    @Override
    public void visitIndex(Index index) {}

    @Override
    public void visitIndexColumn(IndexColumn indexColumn) {}


    @Override
    public void visitJoinColumn(JoinColumn joinColumn) {}

    @Override
    public void visitType(Type type) {}
}
