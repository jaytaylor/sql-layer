
package com.akiban.server.entity.changes;

import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;

import java.util.UUID;

public interface SpaceModificationHandler {
    void addEntity(Entity entity, String name);
    void dropEntity(Entity dropped, String oldName);

    void beginEntity(Entity entity, String name);
    void renameEntity(UUID entityUuid, String oldName);

    void beginAttributes(AttributeLookups oldLookups, AttributeLookups newLookups);
    void addAttribute(UUID attributeUuid);
    void dropAttribute(Attribute dropped);
    void renameAttribute(UUID attributeUuid, String oldName);
    void changeScalarType(UUID scalarUuid, Attribute afterChange);
    void changeScalarValidations(UUID scalarUuid, Attribute afterChange);
    void changeScalarProperties(UUID scalarUuid, Attribute afterChange);
    void endAttributes();

    void addEntityValidation(Validation validation);
    void dropEntityValidation(Validation validation);

    void addIndex(String name);
    void dropIndex(String name, EntityIndex index);
    void renameIndex(EntityIndex index, String oldName, String newName);

    void endEntity();

    void error(String message);
}
