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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public final class Entity {
    public UUID uuid() {
        return uuid;
    }

    @SuppressWarnings("unused")
    void setEntity(String entity) {
        this.uuid = Util.parseUUID(entity);
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    @SuppressWarnings("unused")
    void setAttributes(Map<String, Attribute> attributes) {
        this.attributes = Collections.unmodifiableMap(new TreeMap<>(attributes));
    }

    public Collection<Validation> getValidation() {
        return validations;
    }

    @SuppressWarnings("unused")
    void setValidation(List<Map<String, ?>> validations) {
        this.validations = new ArrayList<>(validations.size());
        for (Map<String, ?> validation : validations) {
            this.validations.add(new Validation(validation));
        }
        this.validations = Collections.unmodifiableList(this.validations);
    }

    public BiMap<String, EntityIndex> getIndexes() {
        return indexes;
    }

    @SuppressWarnings("unused")
    void setIndexes(Map<String, List<List<String>>> indexes) {
        this.indexes = HashBiMap.create(indexes.size());
        for (Map.Entry<String, List<List<String>>> entry : indexes.entrySet()) {
            EntityIndex index = new EntityIndex(entry.getValue());
            if (this.indexes.put(entry.getKey(), index) != null)
                throw new IllegalEntityDefinition("multiple names given for index: " + index);
        }
        this.indexes = ImmutableBiMap.copyOf(this.indexes);
    }

    @Override
    public String toString() {
        return String.format("entity {%s}", uuid());
    }

    private UUID uuid;
    private Map<String, Attribute> attributes = Collections.emptyMap();
    private List<Validation> validations = Collections.emptyList();
    private BiMap<String, EntityIndex> indexes = ImmutableBiMap.of();

    Entity() {}

    public void accept(String myName, EntityVisitor visitor) {
        if (visitor.visitEntity(myName, this)) {
            for (Map.Entry<String, Attribute> entry : attributes.entrySet()) {
                entry.getValue().accept(entry.getKey(), visitor);
            }
            for (Validation validation : validations) {
                validation.accept(visitor);
            }
            for (Map.Entry<String, EntityIndex> entry : new TreeMap<>(indexes).entrySet()) {
                entry.getValue().accept(entry.getKey(), visitor);
            }
            visitor.leaveEntity();
        }
    }
}
