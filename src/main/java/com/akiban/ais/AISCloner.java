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

package com.akiban.ais;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Schema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.AISProtobuf;
import com.akiban.ais.protobuf.ProtobufReader;
import com.akiban.ais.protobuf.ProtobufWriter;

public class AISCloner {
    public static AkibanInformationSchema clone(AkibanInformationSchema ais) {
        return clone(ais, ProtobufWriter.ALL_TABLES_SELECTOR);
    }

    public static AkibanInformationSchema clone(AkibanInformationSchema ais, ProtobufWriter.TableSelector selector) {
        AkibanInformationSchema newAIS = new AkibanInformationSchema();
        clone(newAIS, ais, selector);
        return newAIS;
    }

    public static void clone(AkibanInformationSchema destAIS, AkibanInformationSchema srcAIS, ProtobufWriter.TableSelector selector) {
        ProtobufWriter writer = new ProtobufWriter(selector);
        AISProtobuf.AkibanInformationSchema pbAIS = writer.save(srcAIS);
        ProtobufReader reader = new ProtobufReader(destAIS, pbAIS.toBuilder());
        reader.loadAIS();
        // Preserve the memory table factories for any I_S tables
        Schema schema = destAIS.getSchema(TableName.INFORMATION_SCHEMA);
        if(schema != null) {
            for(UserTable newTable : schema.getUserTables().values()) {
                UserTable oldTable = srcAIS.getUserTable(newTable.getName());
                if(oldTable != null) {
                    newTable.setMemoryTableFactory(oldTable.getMemoryTableFactory());
                }
            }
        }
        // TODO: Limp along GroupTables, again. The rest of this can go away when they do.
        AISBuilder builder = new AISBuilder(destAIS);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        for(Group oldGroup : srcAIS.getGroups().values()) {
            Group newGroup = destAIS.getGroup(oldGroup.getName());
            if(newGroup != null) {
                GroupTable oldTable = oldGroup.getGroupTable();
                GroupTable newTable = newGroup.getGroupTable();
                if(oldTable != null && newTable != null) {
                    newTable.setTableId(oldTable.getTableId());
                }
            }
        }
    }
}
