
package com.akiban.server.entity.model;

import com.akiban.util.JUnitUtils;
import com.google.common.collect.BiMap;

import java.util.Map;
import java.util.Set;

class ToStringVisitor extends JUnitUtils.MessageTaker implements EntityVisitor<RuntimeException> {

    @Override
    public void visitEntity(String name, Entity entity) {
        message("visiting entity", name, entity);
    }

    @Override
    public void leaveEntity() {
        message("leaving entity");
    }

    @Override
    public void visitScalar(String name, Attribute scalar) {
        message("visiting scalar", name, scalar);
    }

    @Override
    public void visitCollection(String name, Attribute collection) {
        message("visiting collection", name, collection);
    }

    @Override
    public void leaveCollection() {
        message("leaving collection");
    }

    @Override
    public void leaveEntityAttributes() {
    }

    @Override
    public void visitIndexes(BiMap<String, EntityIndex> indexes) {
        for (Map.Entry<String, EntityIndex> index : indexes.entrySet())
            message("visiting index", index.getKey(), index.getValue());
    }

    @Override
    public void visitEntityValidations(Set<Validation> validations) {
        for (Validation validation : validations)
            message("visiting entity validation", validation);

    }

}
