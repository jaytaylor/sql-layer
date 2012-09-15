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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Hook for <code>AlterTableRequest</code>.
 * When told to <code>ALTER TABLE _xxx_new ...</code>, if the table currently
 * matches table <code>xxx</code> in the same schema, tag it with information about
 * the alter for later.
 */
public class OSCAlterTableHook
{
    public static final String PROPERTY = "akserver.pt.osc.hook";

    private static final Logger logger = LoggerFactory.getLogger(OSCAlterTableHook.class);

    private final MessageRequiredServices requiredServices;
    
    public OSCAlterTableHook(MessageRequiredServices requiredServices) {
        this.requiredServices = requiredServices;
    }

    public void before(Session session, TableName name, UserTable definition, List<TableChange> columnChanges, List<TableChange> indexChanges) {
        // A copy of table xxx is made as _xxx_new and then altered per the command.
        if (!((name.getTableName().charAt(0) == '_') &&
              name.getTableName().endsWith("_new") &&
              "enabled".equals(requiredServices.config().getProperty(PROPERTY))))
            return;

        AkibanInformationSchema ais = requiredServices.schemaManager().getAis(session);
        UserTable oldDefinition = ais.getUserTable(name);
        if (oldDefinition == null) return;

        String schemaName = name.getSchemaName();
        String tableName = name.getTableName();
        int i = 0;
        while (tableName.charAt(i++) == '_') {
            String origName = tableName.substring(i, tableName.length() - 4);
            UserTable origDefinition = ais.getUserTable(schemaName, origName);
            if ((origDefinition != null) && 
                hasSameColumns(oldDefinition, origDefinition)) {
                definition.setPendingOSC(new PendingOSC(origName, columnChanges, indexChanges));
                logger.info("Change by OSC detected: ALTER TABLE {}{}{} pending",
                            new Object[] { origName, columnChanges, indexChanges });
                return;
            }
        }
    }

    protected boolean hasSameColumns(UserTable t1, UserTable t2) {
        List<Column> cols1 = t1.getColumns();
        List<Column> cols2 = t2.getColumns();
        if (cols1.size() != cols2.size()) 
            return false;
        for (int i = 0; i < cols1.size(); i++) {
            Column col1 = cols1.get(i);
            Column col2 = cols2.get(i);
            if (!(col1.getName().equals(col2.getName()) &&
                  col1.getType().equals(col2.getType())))
                return false;
        }
        return true;
    }
}
