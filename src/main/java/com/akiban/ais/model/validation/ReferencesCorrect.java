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
import com.akiban.server.error.AISNullReferenceException;
import com.akiban.server.error.BadAISInternalSettingException;
import com.akiban.server.error.BadAISReferenceException;

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
            output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException ("ais", "", "user table")));
        } else if (userTable.isGroupTable()) {
            output.reportFailure(new AISValidationFailure(
                    new BadAISInternalSettingException("User table", userTable.getName().toString(), "isGroupTable")));
        }
        
    }

    @Override
    public void visitColumn(Column column) {
        if (column == null) {
            output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException ("user table", visitingTable.getName().toString(), "column")));
        } else if (column.getTable() != visitingTable) {
            output.reportFailure(new AISValidationFailure(
                    new BadAISReferenceException ("column", column.getName(), "table", visitingTable.getName().toString())));
        }
    }

    @Override
    public void visitGroupTable(GroupTable groupTable) {
        visitingTable = groupTable;
        if (groupTable == null) {
            output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException("ais", "", "group table")));
        } else if (groupTable.isUserTable()) {
            output.reportFailure(new AISValidationFailure(
                    new BadAISInternalSettingException ("Group table", groupTable.getName().toString(), "isUserTable")));
        }
    }

    @Override
    public void visitGroup(Group group) {
        visitingGroup = group;
        if (group == null) {
            output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException("ais", "", "group")));
        } else  if (group.getGroupTable() == null) {
            output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException("group", group.getName(), "group table")));
        }
    }


    @Override
    public void visitIndex(Index index) {
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
                            "group", visitingGroup.getName())));
        }
        
    }
    @Override
    public void visitIndexColumn(IndexColumn indexColumn) {
        if (indexColumn == null) {
            output.reportFailure(new AISValidationFailure (
                    new AISNullReferenceException ("index", visitingIndex.getIndexName().toString(), "column")));
        } else if (indexColumn.getIndex() != visitingIndex) {
            output.reportFailure(new AISValidationFailure (
                    new BadAISReferenceException ("Index column",indexColumn.getColumn().getName(), 
                            "index", visitingIndex.getIndexName().toString())));
        }

    }

    @Override
    public void visitJoin(Join join) {}

    @Override
    public void visitJoinColumn(JoinColumn joinColumn) {}

    @Override
    public void visitType(Type type) {}

}
