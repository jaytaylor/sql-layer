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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public class Entity extends EntityElement {

    public List<EntityField> getFields() {
        return fields;
    }

    void setFields(List<EntityField> fields) {
        this.fields = ImmutableList.copyOf(fields);
    }

    /**
     * Returns a map of each field by its UUID, ordered by the field positions. Iterations (key, value and entry-set)
     * will all reflect the field order as it is in {@linkplain #getFields()}. So for instance, this:
     * <pre>    assert getFields().equals(new ArrayList&lt;&gt;(fieldsByUuid().values()));</pre>
     * should <em>not</em> raise an assert
     * @return a map of fields by UUID, ordered by field position
     */
    @JsonIgnore
    public Map<UUID, EntityField> fieldsByUuid() {
        Map<UUID, EntityField> local = fieldsByUuid;
        if (local == null) {
            local = new LinkedHashMap<>(fields.size());
            for (EntityField field : fields) {
                Object old = local.put(field.getUuid(), field);
                assert old == null : field.getUuid();
            }
            local = Collections.unmodifiableMap(local);
            fieldsByUuid = local; // logically idempotent since fields is immutable, so it's fine even if racy
        }
        return local;
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

    @Override
    public String toString() {
        return getName();
    }

    protected void acceptStart(EntityVisitor visitor) {
        visitor.enterTopEntity(this);
    }

    protected void acceptFinish(EntityVisitor visitor) {
        visitor.leaveTopEntity();
    }

    public final void accept(EntityVisitor visitor) {
        acceptStart(visitor);
        if (!collections.isEmpty()) {
            visitor.enterCollections();
            for (EntityCollection collection : collections)
                collection.accept(visitor);
            visitor.leaveCollections();
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
    private transient volatile Map<UUID, EntityField> fieldsByUuid;
}
