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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class SpaceDiff {

    public static <H extends SpaceModificationHandler> H apply(Space original, Space updated, H out) {
        // This is going to be a multi-pass approach.
        // In the first pass, we look at all EntityElements that appear in both spaces. Before we do anything else,
        // we confirm that when an EntityElement appears in both spaces, it is of the same Java class; if not, we
        // output an error and mark that UUID as having been handled (so that subsequent passes will ignore it).
        // If both EntityElements are of the same class, we check if the element moved. If so, we (1) raise an error
        // if it's a field (those can't move) and (2) mark the UUID as having been handled. If the element was an
        // Entity, we create a runnable to handle it in the next pass.
        // In the second pass, we simply run all the runnables created in the previous pass. These handle Entities
        // which exist in both spaces, but with different parents.
        // In the last pass, we traverse each space's entities root-to-leaf. When we get to a drop or add, we don't
        // traverse down, because if we're dropping "customers", we don't also need to mention that we're dropping
        // "orders." We don't want a move to be registered as a drop in one place and an add in another, so we
        // check as we traverse the tree to make sure that UUIDs haven't already been handled.
        final H out_ = out;
        final Set<UUID> handledUuids = new HashSet<>();
        final EntityElementLookups origLookups = new EntityElementLookups(original);
        final EntityElementLookups updatedLookups = new EntityElementLookups(updated);
        final Collection<Runnable> moveHandlers = new ArrayList<>(5); // rough guess for capacity
        final Handler diffHandler = new Handler(handledUuids, origLookups, updatedLookups, out);

        MapDiff.apply(origLookups.getElementsByUuid(), updatedLookups.getElementsByUuid(),
            new MapDiffHandler.Default<UUID, EntityElement>()
        {
            @Override
            public void inBoth(final UUID key, final EntityElement original, final EntityElement updated) {
                boolean origIsField = (original.getClass() == EntityField.class);
                boolean updatedIsField = (updated.getClass() == EntityField.class);
                if (origIsField != updatedIsField) {
                    out_.error("Can't change an element's class (whether it's a field or an entity/collection)");
                    handledUuids.add(key);
                }
                if (!EntityElement.sameByUuid(origLookups.getParent(key), updatedLookups.getParent(key)))
                {
                    handledUuids.add(key);
                    if (origIsField)
                        out_.error("Can't move field");
                    else
                        moveHandlers.add(new Runnable() {
                            @Override
                            public void run() {
                                diffHandler.entityActions(key, (Entity) original, (Entity) updated, true);
                            }
                        });
                }
            }
        });
        for (Runnable moveHandler : moveHandlers)
            moveHandler.run();

        MapDiff.apply(
                entitiesByUuid(original.getEntities(), handledUuids),
                entitiesByUuid(updated.getEntities(), handledUuids),
                diffHandler);
        return out;
    }

    private static class Handler implements MapDiffHandler<UUID, Entity> {

        @Override
        public void added(Entity element) {
            if (!handledUuids.contains(element.getUuid()))
                out.addEntity(element);
        }

        @Override
        public void dropped(Entity element) {
            if (!handledUuids.contains(element.getUuid()))
                out.dropEntity(element);
        }

        @Override
        public void inBoth(UUID uuid, Entity original, Entity updated) {
            entityActions(uuid, original, updated, false);
        }

        public void entityActions(UUID uuid, Entity original, Entity updated, boolean isMoved) {
            out.beginEntity(original, updated);
            if (isMoved) {
                Entity origParent = (Entity) origLookups.getParent(uuid);
                Entity updatedParent = (Entity) origLookups.getParent(uuid);
                out.moveEntity(origParent, updatedParent);
            }
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
            Map<UUID, EntityField> originalFields = original.fieldsByUuid();
            Map<UUID, EntityField> updatedFields = updated.fieldsByUuid();
            final LinkedHashSet<UUID> originalUuids = new LinkedHashSet<>(originalFields.keySet());
            final LinkedHashSet<UUID> updatedUUids = new LinkedHashSet<>(updatedFields.keySet());
            MapDiff.apply(originalFields, updatedFields, new MapDiffHandler<UUID, EntityField>()
            {
                @Override
                public void added(EntityField element) {
                    if (!handledUuids.contains(element.getUuid()))
                        out.addField(element.getUuid());
                    // This wasn't in the original map, so remove its UUID from the updated one to sync them
                    updatedUUids.remove(element.getUuid());
                }

                @Override
                public void dropped(EntityField element) {
                    if (!handledUuids.contains(element.getUuid()))
                        out.dropField(element.getUuid());
                    // This wasn't in the updated map, so remove its UUID from the original one to sync them
                    originalUuids.remove(element.getUuid());
                }

                @Override
                public void inBoth(UUID uuid, EntityField origField, EntityField updatedField) {
                    assert origField.getUuid().equals(updatedField.getUuid()) : origField + " / " + updatedField;
                    if (handledUuids.contains(uuid))
                        return;
                    String origName = origField.getName();
                    if (!origName.equals(updatedField.getName()))
                        out.renameField(origField.getUuid());
                    if (!origField.getType().toLowerCase().equals(updatedField.getType().toLowerCase())) {
                        Entity oldParent = (Entity) origLookups.getParent(uuid);
                        boolean changingSpinal = oldParent.getIdentifying().contains(origName);
                        if (!changingSpinal && (oldParent instanceof EntityCollection)) {
                            EntityCollection collection = (EntityCollection) oldParent;
                            changingSpinal = collection.getGroupingFields().contains(origName);
                        }
                        if (changingSpinal)
                            out.error("Can't change type of identifying fields or grouping fields");
                        else
                            out.changeFieldType(uuid);
                    }
                    if (!origField.getValidations().equals(updatedField.getValidations()))
                        out.changeFieldValidations(uuid);
                    if (!origField.getProperties().equals(updatedField.getProperties()))
                        out.changeFieldProperties(uuid);
                }
            });
            assert originalUuids.equals(updatedUUids) : originalUuids + ", " + updatedUUids;
            Map<UUID, Integer> originalUuidPositions = uuidByPosition(originalUuids);
            int pos = 0;
            for (UUID uuid : updatedUUids) {
                int originalPos = originalUuidPositions.get(uuid);
                int updatedPos = pos++;
                if (originalPos != updatedPos)
                    out.fieldOrderChanged(uuid);
            }
        }

        private Map<UUID, Integer> uuidByPosition(LinkedHashSet<UUID> uuids) {
            Map<UUID, Integer> map = new HashMap<>(uuids.size());
            int pos = 0;
            for (UUID uuid : uuids)
                map.put(uuid, pos++);
            return map;
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
            if ((original instanceof EntityCollection) && (updated instanceof EntityCollection)) {
                EntityCollection origCollection = (EntityCollection) original;
                EntityCollection updatedCollection = (EntityCollection) updated;
                if (!origCollection.getGroupingFields().equals(updatedCollection.getGroupingFields()))
                    out.groupingFieldsChanged();
            }
        }

        private Handler(Set<UUID> handledUuids,
                        EntityElementLookups origLookups,
                        EntityElementLookups updatedLookups,
                        SpaceModificationHandler out)
        {
            this.handledUuids = handledUuids;
            this.origLookups = origLookups;
            this.updatedLookups = updatedLookups;
            this.out = out;
        }

        private final Set<UUID> handledUuids;
        private final SpaceModificationHandler out;
        private final EntityElementLookups origLookups;
        private final EntityElementLookups updatedLookups;
    }

    private SpaceDiff() {}

    private static Map<UUID, Entity> entitiesByUuid(Collection<? extends Entity> entities, Set<UUID> badUuids) {
        Map<UUID, Entity> entityMap = new LinkedHashMap<>(entities.size());
        List<Entity> entitiesSorted = new ArrayList<>(entities);
        Collections.sort(entitiesSorted, EntityElement.byName);
        for (Entity entity : entitiesSorted) {
            UUID uuid = entity.getUuid();
            if (!badUuids.contains(uuid)) {
                Object old = entityMap.put(uuid, entity);
                assert old == null : uuid;
            }
        }
        return entityMap;
    }

    private static Map<UUID, Entity> entitiesByUuid(Collection<? extends Entity> entities) {
        return entitiesByUuid(entities, Collections.<UUID>emptySet());
    }
}
