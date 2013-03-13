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
import com.akiban.server.service.session.Session;

public final class EntityParser {
    private static final Logger LOG = LoggerFactory.getLogger(EntityParser.class);
    
    public EntityParser () {
    }
        
    public UserTable parse (TableName tableName, JsonNode node) throws IOException {
        NewAISBuilder builder = AISBBasedBuilder.create(tableName.getSchemaName());
        processContainer (node, builder, tableName);
        return builder.ais().getUserTable(tableName);
    }

    public UserTable create (final DDLFunctions ddlFunctions, final Session session, UserTable newRoot) throws IOException {
        newRoot.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                ddlFunctions.createTable(session, table);
            }
        });
        return ddlFunctions.getUserTable(session, newRoot.getName());
    }

    public UserTable parseAndCreate (final DDLFunctions ddlFunctions, final Session session,
                                     TableName tableName, JsonNode node) throws IOException {
        return create(ddlFunctions, session, parse(tableName, node));
    }
    
    private void processContainer (JsonNode node, NewAISBuilder builder, TableName tableName) throws IOException {
        boolean first = true;
        if (node.isObject()) {
            processTable (node, builder, tableName);
        } else if (node.isArray()) {
            // For an array of elements, process the first one and discard the rest
            for (JsonNode arrayElement : node) {
                if (first && arrayElement.isObject()) { 
                    processTable(arrayElement, builder, tableName);
                    first = false;
                }
                else if (first && !arrayElement.isContainerNode()) {
                    NewUserTableBuilder table = builder.userTable(tableName.getSchemaName(), tableName.getTableName());
                    addColumnToTable (arrayElement, "value", table);
                    first = false;
                }
                // else throw Bad Json Format Exception
            }
            // If no elements in the array, add a placeholder column
            if (first) {
                NewUserTableBuilder table = builder.userTable(tableName.getSchemaName(), tableName.getTableName());
                table.colString("placeholder", 128, true);
            }
        }
        // else throw Bad Json Format Exception
    }

    private void processTable (JsonNode node, NewAISBuilder builder, TableName tableName) throws IOException {
        
        LOG.trace("Creating Table {}", tableName);
        // Pass one, insert fields from the table
        boolean columnsAdded = false;
        NewUserTableBuilder table = builder.userTable(tableName.getSchemaName(), tableName.getTableName());
        Iterator<Entry<String,JsonNode>> i = node.getFields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isValueNode()) {
                LOG.trace("Column {}", field.getKey());
                addColumnToTable(field.getValue(), field.getKey(), table);
                columnsAdded = true;
            }
        }
        
        if (!columnsAdded) {
            table.colString("placeholder", 128, true);
            LOG.trace("Column added placeholder");
        }
        // pass 2: insert the child nodes
        boolean first = true;
        String columnName = "_" + tableName.getTableName() + "_id";
        i = node.getFields();
        while (i.hasNext()) {
            Entry<String,JsonNode> field = i.next();
            if (field.getValue().isContainerNode()) {
                LOG.trace("Creating child table {} - first {}", field.getKey(), first);
                if (first) {
                    table.autoIncLong(columnName, 0);
                    table.pk(columnName);
                    first = false;
                }
                TableName childTable = TableName.parse(tableName.getSchemaName(), field.getKey());
                processContainer (field.getValue(), builder, childTable);
                NewUserTableBuilder child = builder.getUserTable(childTable);
                child.colLong(columnName);
                LOG.trace("Column added {}", columnName);
                child.joinTo(tableName).on(columnName, columnName);
                builder.getUserTable(tableName);
            }
        }
    }
    
    private void addColumnToTable (JsonNode node, String name, NewUserTableBuilder table) {
        if (node.isTextual()) {
            int  len = Math.max(node.asText().length(), 128);
            table.colString(name, len, true);
        } else if (node.isIntegralNumber()) {
            table.colBigInt(name, true);
        } else if (node.isDouble()) {
            table.colDouble(name, true);
        } else if (node.isBoolean()) {
            table.colLong(name, true);
        } else if (node.isNull()) {
            // wild guess
            table.colString(name, 128, true);
        }
    }
}
