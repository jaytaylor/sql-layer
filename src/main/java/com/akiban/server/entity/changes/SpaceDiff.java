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
import com.akiban.server.entity.model.EntityCollection;
import com.akiban.server.entity.model.EntityElement;
import com.akiban.server.entity.model.EntityField;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Space;
import com.akiban.server.entity.model.Validation;
import com.akiban.util.MapDiff;
import com.akiban.util.MapDiffHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class SpaceDiff {

    public static <H extends SpaceModificationHandler> H apply(Space original, Space updated, H out) {
        MapDiff.apply(entitiesByUuid(original.getEntities()), entitiesByUuid(updated.getEntities()), new Handler(out));
        return out;
    }

    private static class Handler implements MapDiffHandler<UUID, Entity> {

        @Override
        public void added(Entity element) {
            out.addEntity(element);
        }

        @Override
        public void dropped(Entity element) {
            out.dropEntity(element);
        }

        @Override
        public void inBoth(UUID uuid, Entity original, Entity updated) {
            out.beginEntity(original, updated);
            String origName = original.getName();
            String updatedName = updated.getName();
            if (!origName.equals(updatedName))
                out.renameEntity();
            fieldActions(original, updated);
            validationActions(original, updated);
            indexActions(original, updated);
            collectionActions(original, updated);
            out.endEntity();
        }

        private void fieldActions(Entity original, Entity updated) {

            MapDiff.apply(original.fieldsByUuid(), updated.fieldsByUuid(), new MapDiffHandler<UUID, EntityField>()
            {
                @Override
                public void added(EntityField element) {
                    out.addField(element.getUuid());
                }

                @Override
                public void dropped(EntityField element) {
                    out.dropField(element.getUuid());
                }

                @Override
                public void inBoth(UUID uuid, EntityField origField, EntityField updatedField) {
                    assert origField.getUuid().equals(updatedField.getUuid()) : origField + " / " + updatedField;
                    if (!origField.getName().equals(updatedField.getName()))
                        out.renameField(origField.getUuid());
                    if (!origField.getType().toLowerCase().equals(updatedField.getType().toLowerCase()))
                        out.changeFieldType(uuid);
                    if (!origField.getValidations().equals(updatedField.getValidations()))
                        out.changeFieldValidations(uuid);
                    if (!origField.getProperties().equals(updatedField.getProperties()))
                        out.changeFieldProperties(uuid);
                }
            });
        }

        private void validationActions(Entity original, Entity updated) {
            Set<Validation> origValidations = new HashSet<>(original.getValidations());
            Set<Validation> updatedValidations = new HashSet<>(updated.getValidations());
            for (Validation orig : origValidations) {
                if (!updatedValidations.contains(orig))
                    out.dropEntityValidation(orig);
            }
            for (Validation updatedValidation : updatedValidations) {
                if (!origValidations.contains(updatedValidation))
                    out.addEntityValidation(updatedValidation);
            }
        }

        private void indexActions(Entity origial, Entity updated) {
            Map<EntityIndex, String> originalIndexes = origial.getIndexes().inverse();
            Map<EntityIndex, String> updatedIndexes = updated.getIndexes().inverse();

            MapDiff.apply(originalIndexes, updatedIndexes, new MapDiffHandler<EntityIndex, String>() {
                @Override
                public void added(String element) {
                    out.addIndex(element);
                }

                @Override
                public void dropped(String element) {
                    out.dropIndex(element);
                }

                @Override
                public void inBoth(EntityIndex index, String originalName, String updatedName) {
                    if (!originalName.equals(updatedName))
                        out.renameIndex(index);
                }
            });
        }

        private void collectionActions(Entity original, Entity updated) {
            MapDiff.apply(entitiesByUuid(original.getCollections()), entitiesByUuid(updated.getCollections()), this);

            if (!original.getIdentifying().equals(updated.getIdentifying()))
                out.identifyingFieldsChanged();
            boolean isCollection = (original instanceof EntityCollection);
            assert isCollection == (updated instanceof EntityCollection) : original + " / " + updated;
            if (isCollection) {
                EntityCollection origCollection = (EntityCollection) original;
                EntityCollection updatedCollection = (EntityCollection) updated;
                if (!origCollection.getGroupingFields().equals(updatedCollection.getGroupingFields()))
                    out.groupingFieldsChanged();
            }
        }

        private Handler(SpaceModificationHandler out) {
            this.out = out;
        }

        private final SpaceModificationHandler out;
    }

    private SpaceDiff() {}

    private static Map<UUID, Entity> entitiesByUuid(Collection<? extends Entity> entities) {
        Map<UUID, Entity> entityMap = new LinkedHashMap<>(entities.size());
        List<Entity> entitiesSorted = new ArrayList<>(entities);
        Collections.sort(entitiesSorted, EntityElement.byName);
        for (Entity entity : entitiesSorted) {
            Object old = entityMap.put(entity.getUuid(), entity);
            assert old == null : entity.getUuid();
        }
        return entityMap;
    }
}
