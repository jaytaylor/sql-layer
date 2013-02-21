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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.ais.model.aisb2.NewUserTableBuilder;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;

public final class EntityParser {

    private DDLFunctions  ddlFunctions;
    private static final Logger LOG = LoggerFactory.getLogger(EntityParser.class);
    
    public EntityParser (DXLService dxlService) {
        this.ddlFunctions = dxlService.ddlFunctions();
    }    
        
    public Response parse (final Session session, TableName tableName, JsonNode node) throws IOException {

        NewAISBuilder builder = AISBBasedBuilder.create(tableName.getSchemaName());

        processContainer (node, builder, tableName);

        UserTable table = builder.ais().getUserTable(tableName);

        table.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                ddlFunctions.createTable(session, table);
                
            }
        });
        
        return null;
    }
    
    private void processContainer (JsonNode node, NewAISBuilder builder, TableName tableName) throws IOException {
        if (node.isObject()) {
            processTable (node, builder, tableName);
        } else if (node.isArray()) {
            assert false: "JSON array not supported";
        }
    }

    private void processTable (JsonNode node, NewAISBuilder builder, TableName tableName) throws IOException {
        // Pass one, insert fields from the table
        NewUserTableBuilder table = builder.userTable(tableName.getSchemaName(), tableName.getTableName());
        Iterator<Entry<String,JsonNode>> i = node.getFields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isValueNode()) {
                
                if (field.getValue().isTextual()) {
                    int  len = Math.min(field.getValue().asText().length(), 128);
                    table.colString(field.getKey(), len, true);
                } else if (field.getValue().isIntegralNumber()) {
                    table.colBigInt(field.getKey(), true);
                } else if (field.getValue().isDouble()) {
                    table.colDouble(field.getKey(), true);
                } else if (field.getValue().isBoolean()) {
                    table.colLong(field.getKey(), true);
                } else if (field.getValue().isNull()) {
                    // wild guess
                    table.colString(field.getKey(), 128, true);
                }
            }
        }
        // pass 2: insert the child nodes
        boolean first = true;
        String columnName = "_" + tableName.getTableName() + "_id";
        i = node.getFields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isContainerNode()) {
                if (first) {
                    table.autoIncLong(columnName, 0);
                    table.pk(columnName);
                    first = false;
                }
                TableName childTable = TableName.parse(tableName.getSchemaName(), field.getKey());
                processContainer (field.getValue(), builder, childTable);
                NewUserTableBuilder child = builder.getUserTable();
                child.colLong(columnName);
                child.joinTo(tableName).on(columnName, columnName);
            }
        }
    }
}
