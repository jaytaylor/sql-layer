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

import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;
import com.akiban.util.JUnitUtils;

import java.util.UUID;

public class StringChangeLog extends JUnitUtils.MessageTaker implements SpaceModificationHandler {
    @Override
    public void beginEntity(UUID entityUUID, AttributeLookups oldLookups, AttributeLookups newLookups) {
        // None
    }

    @Override
    public void endEntity() {
        // None
    }

    @Override
    public void addEntity(UUID entityUuid) {
        message("add entity", entityUuid);
    }

    @Override
    public void dropEntity(Entity dropped, String oldName) {
        message("drop entity", dropped.uuid(), oldName);
    }

    @Override
    public void renameEntity(UUID entityUuid, String oldName) {
        message("rename entity", entityUuid, oldName);
    }

    @Override
    public void addAttribute(UUID parentAttributeUuid, UUID attributeUuid) {
        message("add attribute", attributeUuid);
    }

    @Override
    public void dropAttribute(UUID parentAttributeUuid, String oldName, Attribute dropped) {
        message("drop attribute", dropped.getUUID());
    }

    @Override
    public void renameAttribute(UUID attributeUuid, String oldName) {
        message("rename attribute", attributeUuid, oldName);
    }

    @Override
    public void changeScalarType(UUID scalarUuid, Attribute afterChange) {
        message("change scalar type", scalarUuid, afterChange.getType());
    }

    @Override
    public void changeScalarValidations(UUID scalarUuid, Attribute afterChange) {
        message("change scalar validation", scalarUuid, afterChange.getValidation());
    }

    @Override
    public void changeScalarProperties(UUID scalarUuid, Attribute afterChange) {
        message("change scalar properties", scalarUuid, afterChange.getProperties());
    }

    @Override
    public void addEntityValidation(Validation validation) {
        message("add entity validation", validation);
    }

    @Override
    public void dropEntityValidation(Validation validation) {
        message("drop entity validation", validation);
    }

    @Override
    public void addIndex(String name) {
        message("add index", name);
    }

    @Override
    public void dropIndex(String name, EntityIndex index) {
        message("drop index", name, index);
    }

    @Override
    public void renameIndex(EntityIndex index, String oldName, String newName) {
        message("rename index", oldName, newName);
    }

    @Override
    public void error(String message) {
        message("error", message);
    }
}
