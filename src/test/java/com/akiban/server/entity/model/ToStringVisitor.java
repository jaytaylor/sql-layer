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
    public void leaveTopEntity() {
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
