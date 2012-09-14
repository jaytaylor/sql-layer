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

package com.akiban.ais.pt;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.TableChange;
import com.akiban.message.MessageRequiredServices;
import com.akiban.server.service.session.Session;

import java.util.List;
import java.util.Map;

public class OSCRenameTableHook
{
    private final MessageRequiredServices requiredServices;

    public OSCRenameTableHook(MessageRequiredServices requiredServices) {
        this.requiredServices = requiredServices;
    }

    public boolean before(Session session, TableName oldName, TableName newName) {
        if ((newName.getTableName().charAt(0) == '_') &&
            newName.getTableName().endsWith("_old") &&
            newName.getSchemaName().equals(oldName.getSchemaName())) {
            return beforeOld(session, oldName, newName);
        }
        else if ((oldName.getTableName().charAt(0) == '_') &&
                 oldName.getTableName().endsWith("_new") &
                 oldName.getSchemaName().equals(newName.getSchemaName())) {
            return beforeNew(session, oldName, newName);
        }
        return true;            // Allow rename to continue.
    }

    protected boolean beforeOld(Session session, TableName oldName, TableName newName) {
        // We are being told to rename xxx to _xxx_old.
        // If this is OSC, there is an _xxx_new that points to xxx.
        // Except that either one of those _ names might have multiple _'s.
        
        AkibanInformationSchema ais = requiredServices.schemaManager().getAis(session);
        String schemaName = oldName.getSchemaName();
        String tableName = oldName.getTableName();

        // Easy case first.
        UserTable table = ais.getUserTable(schemaName, "_" + tableName + "_new");
        if ((table != null) &&
            (table.getPendingOSC() != null) &&
            (table.getPendingOSC().getOriginalName().equals(tableName))) {
            table.getPendingOSC().setCurrentName(newName.getTableName());
            return true;
        }
        
        // Try harder.
        for (Map.Entry<String,UserTable> entry : ais.getSchema(schemaName).getUserTables().entrySet()) {
            if (entry.getKey().contains(tableName) &&
                (table.getPendingOSC() != null) &&
                (table.getPendingOSC().getOriginalName().equals(tableName))) {
                table.getPendingOSC().setCurrentName(newName.getTableName());
                return true;
            }
        }

        return true;
    }

    protected boolean beforeNew(Session session, TableName oldName, TableName newName) {
        return true;
    }

}
