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

package com.akiban.server.entity.changes;

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;

import static com.akiban.ais.protobuf.ProtobufWriter.WriteSelector;
import static com.akiban.ais.protobuf.ProtobufWriter.SingleSchemaSelector;

public class AISDumper {
    public static String dumpDeterministicAIS(AkibanInformationSchema ais, String schema) {
        WriteSelector selector = new SingleSchemaSelector(schema);
        AkibanInformationSchema clone = AISCloner.clone(ais, selector);
        for(UserTable table : clone.getUserTables().values()) {
            table.setTableId(-1);
            table.setVersion(null);
            for(Column column : table.getColumnsIncludingInternal()) {
                column.clearMaxAndPrefixSize();
                if(column.getName().endsWith("_ref") || column.getName().endsWith("_ref$1")) {
                    column.setUuid(null);
                }
            }
            for(Index index : table.getIndexes()) {
                index.setIndexId(-1);
                index.setTreeName(null);
            }
        }
        for(Group group : clone.getGroups().values()) {
            group.setTreeName(null);
            for(Index index : group.getIndexes()) {
                index.setIndexId(-1);
                index.setTreeName(null);
            }
        }
        return new ProtobufWriter(selector).save(clone).toString();
    }
}
