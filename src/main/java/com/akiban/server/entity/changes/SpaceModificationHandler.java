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

package com.akiban.server.entity.changes;

import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityField;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;

import java.util.UUID;

public interface SpaceModificationHandler {
    void addEntity(Entity entity);
    void dropEntity(Entity dropped);

    void beginEntity(Entity oldEntity, Entity newEntity);
    void moveEntity(Entity oldParent, Entity newParent);
    void renameEntity();
    void identifyingFieldsChanged();
    void groupingFieldsChanged();
    void addField(UUID fieldUuid);
    void dropField(UUID fieldUuid);
    void fieldOrderChanged(UUID fieldUuid);
    void renameField(UUID fieldUuid);
    void changeFieldType(UUID fieldUuid);
    void changeFieldValidations(UUID fieldUuid);
    void changeFieldProperties(UUID fieldUuid);
    void addEntityValidation(Validation validation);
    void dropEntityValidation(Validation validation);
    void addIndex(String name);
    void dropIndex(String name);
    void renameIndex(EntityIndex index);
    void endEntity();

    void endTopLevelEntity();

    void error(String message);
}
