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

package com.akiban.server.entity.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class Entity {
    public UUID uuid() {
        return uuid;
    }

    @SuppressWarnings("unused")
    public void setEntity(String entity) {
        this.uuid = Util.parseUUID(entity);
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    @SuppressWarnings("unused")
    public void setAttributes(Map<String, Attribute> attributes) {
        this.attributes = attributes;
    }

    public List<Validation> getValidation() {
        return validations;
    }

    @SuppressWarnings("unused")
    public void setValidation(List<Map<String, ?>> validations) {
        this.validations = new ArrayList<>(validations.size());
        for (Map<String, ?> validation : validations) {
            this.validations.add(Validation.create(validation));
        }
    }

    public Map<EntityIndex, String> getIndexes() {
        return indexes;
    }

    @SuppressWarnings("unused")
    public void setIndexes(Map<String, List<List<String>>> indexes) {
        this.indexes = new HashMap<>(indexes.size());
        for (Map.Entry<String, List<List<String>>> entry : indexes.entrySet()) {
            EntityIndex index = EntityIndex.create(entry.getValue());
            if (this.indexes.put(index, entry.getKey()) != null)
                throw new IllegalEntityDefinition("multiple names given for index: " + index);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entity entity = (Entity) o;

        return Objects.equals(uuid, entity.uuid)
                && attributes.equals(entity.attributes)
                && indexes.equals(entity.indexes)
                && validations.equals(entity.validations);
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (validations != null ? validations.hashCode() : 0);
        result = 31 * result + (indexes != null ? indexes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("entity {%s}", uuid());
    }

    private UUID uuid;
    private Map<String, Attribute> attributes = Collections.emptyMap();
    private List<Validation> validations = Collections.emptyList();
    private Map<EntityIndex, String> indexes = Collections.emptyMap();

    private Entity() {}

    public void accept(String myName, EntityVisitor visitor) {
        visitor.visitEntity(myName, this);
        for (Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            entry.getValue().accept(entry.getKey(), visitor);
        }
        for (Validation validation : validations) {
            validation.accept(visitor);
        }
        for (Map.Entry<EntityIndex, String> entry : indexes.entrySet()) {
            entry.getKey().accept(entry.getValue(), visitor);
        }
        visitor.leaveEntity();
    }
}
