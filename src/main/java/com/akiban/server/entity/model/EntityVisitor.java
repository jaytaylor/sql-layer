
package com.akiban.server.entity.model;

import com.google.common.collect.BiMap;

import java.util.Set;

public interface EntityVisitor<E extends Exception> {
    void visitEntity(String name, Entity entity) throws E;
    void leaveEntity() throws E;
    void visitScalar(String name, Attribute scalar) throws E;
    void visitCollection(String name, Attribute collection) throws E;
    void leaveCollection() throws E;
    void leaveEntityAttributes() throws E;
    void visitEntityValidations(Set<Validation> validations) throws E;
    void visitIndexes(BiMap<String, EntityIndex> indexes) throws E;
}
