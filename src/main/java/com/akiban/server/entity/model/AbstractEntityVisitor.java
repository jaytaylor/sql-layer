
package com.akiban.server.entity.model;

import com.google.common.collect.BiMap;

import java.util.Set;

public abstract class AbstractEntityVisitor implements EntityVisitor<RuntimeException> {
    @Override
    public void visitEntity(String name, Entity entity) {
    }

    @Override
    public void leaveEntity() {
    }

    @Override
    public void visitScalar(String name, Attribute scalar) {
    }

    @Override
    public void visitCollection(String name, Attribute collection) {
    }

    @Override
    public void leaveCollection() {
    }

    @Override
    public void leaveEntityAttributes() {
    }

    @Override
    public void visitEntityValidations(Set<Validation> validations) {
    }

    @Override
    public void visitIndexes(BiMap<String, EntityIndex> indexes) {
    }
}
