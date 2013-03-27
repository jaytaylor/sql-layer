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
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Space;
import com.akiban.server.entity.model.Validation;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public final class SpaceDiff {

    public void apply(SpaceModificationHandler out) {
        Set<UUID> inBoth = new HashSet<>();
        // dropped entities
        for (Map.Entry<UUID, Entity> orig : originalEntities.entitiesByUuid().entrySet()) {
            UUID uuid = orig.getKey();
            if (updatedEntities.containsUuid(uuid))
                inBoth.add(uuid);
            else
                out.dropEntity(orig.getValue(), originalEntities.getName(uuid));
        }
        // new entities
        for (Map.Entry<UUID, Entity> updated : updatedEntities.entitiesByUuid().entrySet()) {
            UUID uuid = updated.getKey();
            if (!originalEntities.containsUuid(uuid))
                out.addEntity(updated.getValue(), updatedEntities.getName(uuid));
        }
        for (UUID uuid : inBoth) {
            out.beginEntity(updatedEntities.getEntity(uuid), updatedEntities.getName(uuid));
            if (!originalEntities.getName(uuid).equals(updatedEntities.getName(uuid)))
                out.renameEntity(uuid, originalEntities.getName(uuid));
            attributeActions(uuid, out);
            validationActions(uuid, out);
            indexActions(uuid, out);
            out.endEntity();
        }
    }

    private void attributeActions(UUID entityUUID, SpaceModificationHandler out) {
        AttributeLookups origLookups = new AttributeLookups(originalEntities.getEntity(entityUUID));
        AttributeLookups updateLookups = new AttributeLookups(updatedEntities.getEntity(entityUUID));
        out.beginAttributes(origLookups, updateLookups);

        // added attributes
        Set<UUID> inBoth = new HashSet<>();
        for (Map.Entry<UUID, Attribute> orig : origLookups.getAttributesByUuid().entrySet()) {
            UUID uuid = orig.getKey();
            if (updateLookups.containsAttribute(uuid)) {
                inBoth.add(uuid);
            }
            else {
                UUID parent = origLookups.getParentAttribute(uuid);
                if (parent == null || updateLookups.containsAttribute(parent)) {
                    Attribute droppedAttribute = orig.getValue();
                    if (droppedAttribute.isSpinal())
                        out.error("Can't drop spinal attribute");
                    else
                        out.dropAttribute(droppedAttribute);
                }
            }
        }
        // dropped attributes
        for (Map.Entry<UUID, Attribute> updated : updateLookups.getAttributesByUuid().entrySet()) {
            UUID uuid = updated.getKey();
            if (!origLookups.containsAttribute(uuid)) {
                UUID parent = updateLookups.getParentAttribute(uuid);
                if (parent == null || origLookups.containsAttribute(parent)) {
                    if (updated.getValue().isSpinal())
                        out.error("Can't add spinal attributes to entities or collections");
                    else
                        out.addAttribute(uuid);
                }
            }
        }
        // modified
        for (UUID uuid : inBoth) {
            if (!origLookups.pathFor(uuid).equals(updateLookups.pathFor(uuid))) {
                out.error("Can't move attribute");
                continue;
            }
            Attribute orig = origLookups.attributeFor(uuid);
            Attribute updated = updateLookups.attributeFor(uuid);
            if (!origLookups.nameFor(uuid).equals(updateLookups.nameFor(uuid))) {
                if (orig.isSpinal())
                    out.error("Can't rename spinal attributes");
                else
                    out.renameAttribute(uuid, origLookups.nameFor(uuid));
            }
            if (!Objects.equals(orig.getAttributeType(), updated.getAttributeType())) {
                out.error("Can't change an attribute's class (scalar or collection)");
            }
            else if (orig.getAttributeType() == Attribute.AttributeType.SCALAR) {
                if (orig.getSpinePos() != updated.getSpinePos()) {
                    // assume at least one of them isSpinal; otherwise they're both -1.
                    if (orig.isSpinal() && updated.isSpinal())
                        out.error("Can't change order of spinal attributes");
                    else if (orig.isSpinal())
                        out.error("Can't make spinal attribute non-spinal");
                    else
                        out.error("Can't make non-spinal attribute spinal");
                }
                if (!lc(orig.getType()).equals(lc(updated.getType()))) {
                    if (orig.isSpinal())
                        out.error("Can't change type of spinal attributes");
                    else
                        out.changeScalarType(uuid, updated);
                }
                if (!orig.getValidation().equals(updated.getValidation()))
                    out.changeScalarValidations(uuid, updated);
                if (!orig.getProperties().equals(updated.getProperties())) {
                    if (orig.isSpinal())
                        out.error("Can't change properties of spinal attributes");
                    else
                        out.changeScalarProperties(uuid, updated);
                }
            }
            else if (orig.getAttributeType() == Attribute.AttributeType.COLLECTION) {
                // do nothing -- the visitor will have captured children
            }
            else {
                throw new AssertionError("unknown attribute class: " + orig.getAttributeType());
            }
        }
        out.endAttributes();
    }

    private static String lc(String string) {
        return string.toLowerCase();
    }

    private void validationActions(UUID entityUUID, SpaceModificationHandler out) {
        Set<Validation> origValidations = new HashSet<>(originalEntities.getEntity(entityUUID).getValidation());
        Set<Validation> updatedValidations = new HashSet<>(updatedEntities.getEntity(entityUUID).getValidation());
        for (Validation orig : origValidations) {
            if (!updatedValidations.contains(orig))
                out.dropEntityValidation(orig);
        }
        for (Validation updated : updatedValidations) {
            if (!origValidations.contains(updated))
                out.addEntityValidation(updated);
        }
    }

    private void indexActions(UUID uuid, SpaceModificationHandler out) {
        Map<EntityIndex, String> originalIndexes = originalEntities.getEntity(uuid).getIndexes().inverse();
        Map<EntityIndex, String> updatedIndexes = updatedEntities.getEntity(uuid).getIndexes().inverse();
        for (Map.Entry<EntityIndex, String> origEntry : originalIndexes.entrySet()) {
            EntityIndex origIndex = origEntry.getKey();
            String origName = origEntry.getValue();
            if (!updatedIndexes.containsKey(origIndex))
                out.dropIndex(origName, origIndex);
            else if (!origName.equals(updatedIndexes.get(origIndex)))
                out.renameIndex(origIndex, origName, updatedIndexes.get(origIndex));
        }
        for (Map.Entry<EntityIndex, String> updatedIndex : updatedIndexes.entrySet()) {
            if (!originalIndexes.containsKey(updatedIndex.getKey()))
                out.addIndex(updatedIndex.getValue());
        }
    }

    public SpaceDiff(Space original, Space update) {
        originalEntities = new SpaceLookups(original);
        updatedEntities = new SpaceLookups(update);
    }

    private final SpaceLookups originalEntities;
    private final SpaceLookups updatedEntities;
}
