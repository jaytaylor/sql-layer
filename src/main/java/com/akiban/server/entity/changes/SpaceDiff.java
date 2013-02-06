/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.entity.changes;

import com.akiban.server.entity.model.Attribute;
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
                out.dropEntity(orig.getValue());
        }
        // new entities
        for (UUID uuid : updatedEntities.keySet()) {
            if (!originalEntities.containsUuid(uuid))
                out.addEntity(uuid);
        }
        for (UUID uuid : inBoth) {
            if (!originalEntities.getName(uuid).equals(updatedEntities.getName(uuid)))
                out.renameEntity(uuid, updatedEntities.getName(uuid));
            attributeActions(uuid, out);
            validationActions(uuid, out);
            indexActions(uuid, out);
        }
    }

    private void attributeActions(UUID entityUUID, SpaceModificationHandler out) {
        AttributeLookups origLookups = new AttributeLookups(originalEntities.getEntity(entityUUID));
        AttributeLookups updateLookups = new AttributeLookups(updatedEntities.getEntity(entityUUID));

        // added attributes
        Set<UUID> inBoth = new HashSet<>();
        for (Map.Entry<UUID, Attribute> orig : origLookups.getAttributesByUuid().entrySet()) {
            UUID uuid = orig.getKey();
            if (updateLookups.containsAttribute(uuid))
                inBoth.add(uuid);
            else
                out.dropAttribute(orig.getValue());
        }
        // dropped attributes
        for (UUID uuid : updateLookups.getAttributesByUuid().keySet()) {
            if (!origLookups.containsAttribute(uuid))
                out.addAttribute(uuid);
        }
        // modified
        for (UUID uuid : inBoth) {
            if (!origLookups.pathFor(uuid).equals(updateLookups.pathFor(uuid))) {
                out.error("moving an attribute is unsupported");
                continue;
            }
            if (!origLookups.nameFor(uuid).equals(updateLookups.nameFor(uuid)))
                out.renameAttribute(uuid, origLookups.nameFor(uuid));
            Attribute orig = origLookups.attributeFor(uuid);
            Attribute updated = updateLookups.attributeFor(uuid);
            if (!Objects.equals(orig.getAttributeType(), updated.getAttributeType())) {
                out.error("can't change an attribute's class (scalar or collection)");
            }
            else if (orig.getAttributeType() == Attribute.AttributeType.SCALAR) {
                if (!orig.getType().equals(updated.getType()))
                    out.changeScalarType(uuid, updated);
                if (!orig.getValidation().equals(updated.getValidation()))
                    out.changeScalarValidations(uuid, updated);
                if (!orig.getProperties().equals(updated.getProperties()))
                    out.changeScalarProperties(uuid, updated);
            }
            else if (orig.getAttributeType() == Attribute.AttributeType.COLLECTION) {
                // do nothing -- the visitor will have captured children
            }
            else {
                throw new AssertionError("unknown attribute class: " + orig.getAttributeType());
            }
        }
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
        for (EntityIndex updatedIndex : updatedIndexes.keySet()) {
            if (!originalIndexes.containsKey(updatedIndex))
                out.addIndex(updatedIndex);
        }
    }

    public SpaceDiff(Space original, Space update) {
        originalEntities = new SpaceLookups(original);
        updatedEntities = new SpaceLookups(update);
    }

    private final SpaceLookups originalEntities;
    private final SpaceLookups updatedEntities;
}
