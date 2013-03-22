/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
