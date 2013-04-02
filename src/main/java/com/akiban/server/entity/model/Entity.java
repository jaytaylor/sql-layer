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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public class Entity extends EntityElement {

    public List<EntityField> getFields() {
        return fields;
    }

    public void setFields(List<EntityField> fields) {
        this.fields = ImmutableList.copyOf(fields);
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Collection<EntityCollection> getCollections() {
        return collections;
    }

    public void setCollections(List<EntityCollection> collections) {
        this.collections = ImmutableList.copyOf(collections);
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getIdentifying() {
        return identifying;
    }

    public void setIdentifying(List<String> identifying) {
        this.identifying = identifying;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public BiMap<String, EntityIndex> getIndexes() {
        return indexes;
    }

    @SuppressWarnings("unused")
    void setIndexes(Map<String, EntityIndex> indexes) {
        this.indexes = HashBiMap.create(indexes.size());
        for (Map.Entry<String, EntityIndex> entry : indexes.entrySet()) {
            if (this.indexes.put(entry.getKey(), entry.getValue()) != null)
                throw new IllegalEntityDefinition("multiple names given for index: " + entry);
        }
        this.indexes = ImmutableBiMap.copyOf(this.indexes);
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Set<Validation> getValidations() {
        return validations;
    }

    void setValidations(Collection<Validation> validations) {
        Set<Validation> validationsSet = new TreeSet<>(validations);
        if (validationsSet.size() != validations.size())
            throw new IllegalEntityDefinition("duplicate validations given: " + validations);
        this.validations = ImmutableSet.copyOf(validationsSet);
    }

    void makeModifiable(UUID uuid) {
        setUuid(uuid);
        fields = new ArrayList<>();
        collections = new ArrayList<>();
        identifying = new ArrayList<>();
        indexes = HashBiMap.create();
        validations = new TreeSet<>();
    }

    protected <E extends Exception> void acceptStart(EntityVisitor<E> visitor) throws E {
        visitor.enterTopEntity(this);
    }

    protected <E extends Exception> void acceptFinish(EntityVisitor<E> visitor) throws E {
        visitor.leaveTopEntity();
    }

    public final  <E extends Exception> void accept(EntityVisitor<E> visitor) throws E {
        acceptStart(visitor);
        if (!collections.isEmpty()) {
            visitor.enterCollections();
            for (EntityCollection collection : collections)
                collection.accept(visitor);
            visitor.enterCollections();
        }
        acceptFinish(visitor);
    }

    public static Entity modifiableEntity(UUID uuid) {
        Entity result = new Entity();
        result.makeModifiable(uuid);
        return result;
    }

    private List<EntityField> fields = Collections.emptyList();
    private List<EntityCollection> collections = Collections.emptyList();
    private List<String> identifying = Collections.emptyList();
    private BiMap<String, EntityIndex> indexes = ImmutableBiMap.of();
    private Set<Validation> validations = Collections.emptySet();
}
