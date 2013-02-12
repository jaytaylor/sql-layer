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

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.entity.model.AbstractEntityVisitor;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;
import com.google.common.collect.BiMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EntityToAIS extends AbstractEntityVisitor {
    private final String schemaName;
    private final AISBuilder builder = new AISBuilder();
    private final List<TableInfo> tableInfoStack = new ArrayList<>();
    private TableInfo curTable = null;

    public EntityToAIS(String schemaName) {
        this.schemaName = schemaName;
    }

    //
    // EntityVisitor
    //

    @Override
    public void visitEntity(String name, Entity entity) {
        beginTable(name, entity.uuid());
    }

    @Override
    public void leaveEntity() {
        endTable();
    }

    @Override
    public void visitScalar(String name, Attribute scalar) {
        // TODO: Lookup properties based on type
        Map<String,Object> props = scalar.getProperties();
        String charset = (String)props.get("charset");
        String collation = (String)props.get("collation");
        Long param1 = (Long)props.get("max_length");
        if(param1 == null) {
            param1 = (Long)props.get("precision");
        }
        Long param2 = (Long)props.get("scale");
        Column column = builder.column(schemaName,
                                       curTableName(),
                                       name,
                                       nextColPos(),
                                       scalar.getType(),
                                       param1,
                                       param2,
                                       true  /*nullable*/,
                                       false /*autoinc*/,
                                       charset,
                                       collation
                                       /*,default*/);
        column.setUuid(scalar.getUUID());

        if(scalar.isSpinal()) {
            addSpinalColumn(name);
        }
    }

    @Override
    public void visitCollection(String name, Attribute collection) {
        beginTable(name, collection.getUUID());
    }

    @Override
    public void leaveCollection() {
        endTable();
    }

    @Override
    public void leaveEntityAttributes() {
        // None
    }

    @Override
    public void visitEntityValidations(Set<Validation> validations) {
        // TODO
    }

    @Override
    public void visitIndexes(BiMap<String, EntityIndex> indexes) {
        // TODO
    }

    //
    // AISBuilderVisitor
    //

    public AkibanInformationSchema getAIS() {
        return builder.akibanInformationSchema();
    }

    private void beginTable(String name, UUID uuid) {
        curTable = new TableInfo(name);
        tableInfoStack.add(curTable);
        UserTable table = builder.userTable(schemaName, name);
        table.setUuid(uuid);
    }

    private void endTable() {
        // Create PRIMARY
        if(!curTable.spinalCols.isEmpty()) {
            builder.index(schemaName, curTableName(), Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
            int pos = 0;
            for(String column : curTable.spinalCols) {
                builder.indexColumn(schemaName, curTableName(), Index.PRIMARY_KEY_CONSTRAINT, column, pos++, true, null);
            }
        }
        // Reset current
        tableInfoStack.remove(tableInfoStack.size() - 1);
        curTable = tableInfoStack.isEmpty() ? null : tableInfoStack.get(tableInfoStack.size() - 1);
    }

    private int nextColPos() {
        return curTable.nextColPos++;
    }

    private String curTableName() {
        return curTable.name;
    }

    private void addSpinalColumn(String name) {
        curTable.spinalCols.add(name);
    }

    private static class TableInfo {
        public final String name;
        public final List<String> spinalCols;
        public int nextColPos;

        public TableInfo(String name) {
            this.name = name;
            this.spinalCols = new ArrayList<>(1);
        }
    }
}