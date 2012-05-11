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
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.GroupMissingTableColumnException;
import com.akiban.server.error.TableColumnNotInGroupException;
/**
 * Verifies that all the table columns are also present in the corresponding group table.
 * 
 * @author tjoneslo
 *
 */
class TableColumnsMatchGroupColumns implements AISValidation {

    NameGenerator name = new DefaultNameGenerator();
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            // table doesn't have a group, this is caught by TablesInAGroup
            if (table.getGroup() == null) { continue; }
            GroupTable groupTable = table.getGroup().getGroupTable();
            
            for (Column column : table.getColumnsIncludingInternal()) {
                if (column.getGroupColumn() == null) {
                    output.reportFailure(new AISValidationFailure (
                            new TableColumnNotInGroupException (table.getName(),column.getName())));
                }
                String groupColumnName = name.generateColumnName(column);
                if (groupTable.getColumn(groupColumnName) == null) {
                    output.reportFailure(new AISValidationFailure (
                            new GroupMissingTableColumnException (groupTable.getName(), table.getName(), column.getName())));
                }
            }
        }
    }
}
