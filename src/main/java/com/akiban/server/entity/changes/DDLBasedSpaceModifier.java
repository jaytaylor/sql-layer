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

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Space;
import com.akiban.server.entity.model.Validation;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;

import java.util.Collection;
import java.util.UUID;

public class DDLBasedSpaceModifier implements SpaceModificationHandler {
    private final DDLFunctions ddlFunctions;
    private final Session session;
    private final String schemaName;
    private final Space space;
    private final SpaceLookups spaceLookups;

    public DDLBasedSpaceModifier(DDLFunctions ddlFunctions, Session session, String schemaName, Space space) {
        this.ddlFunctions = ddlFunctions;
        this.session = session;
        this.schemaName = schemaName;
        this.space = space;
        this.spaceLookups = new SpaceLookups(space);
    }

    @Override
    public void addEntity(UUID entityUuid) {
        Entity entity = spaceLookups.getEntity(entityUuid);
        String entityName = spaceLookups.getName(entityUuid);

        EntityToAIS visitor = new EntityToAIS(schemaName);
        entity.accept(entityName, visitor);

        UserTable root = visitor.getAIS().getUserTable(schemaName, entityName);
        root.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                ddlFunctions.createTable(session, table);
            }
        });

        Collection<GroupIndex> groupIndexes = root.getGroup().getIndexes();
        if(!groupIndexes.isEmpty()) {
            ddlFunctions.createIndexes(session, groupIndexes);
        }
    }

    @Override
    public void dropEntity(Entity dropped, String oldName) {
        ddlFunctions.dropGroup(session, new TableName(schemaName, oldName));
    }

    @Override
    public void renameEntity(UUID entityUuid, String oldName) {
        String newName = spaceLookups.getName(entityUuid);
        ddlFunctions.renameTable(session, new TableName(schemaName, oldName), new TableName(schemaName, newName));
    }

    @Override
    public void addAttribute(UUID attributeUuid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropAttribute(Attribute dropped) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameAttribute(UUID attributeUuid, String oldName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeScalarType(UUID scalarUuid, Attribute afterChange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeScalarValidations(UUID scalarUuid, Attribute afterChange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeScalarProperties(UUID scalarUuid, Attribute afterChange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addEntityValidation(Validation validation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropEntityValidation(Validation validation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addIndex(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropIndex(String name, EntityIndex index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameIndex(EntityIndex index, String oldName, String newName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void error(String message) {
        throw new UnsupportedOperationException();
    }
}
