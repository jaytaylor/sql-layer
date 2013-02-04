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

package com.akiban.server.entity;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public final class Entity {

    public static Entity create(Reader reader) throws IOException {
        Entity result = new ObjectMapper().readValue(reader, Entity.class);
        result.validate(new HashSet<UUID>());
        return result;
    }

    private void validate(Set<? super UUID> uuids) {
        if (uuid == null)
            throw new IllegalEntityDefinition("no uuid specified");
        if (!uuids.add(uuid))
            throw new IllegalEntityDefinition("duplicate uuid found: " + uuid);
        for (Attribute attribute : attributes.values())
            attribute.validate(uuids);
    }

    public UUID uuid() {
        return uuid;
    }

    public void setEntity(String entity) {
        this.uuid = Util.parseUUID(entity);
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Attribute> attributes) {
        this.attributes = attributes;
    }

    public List<Map<String, ?>> getValidation() {
        return validations;
    }

    public void setValidation(List<Map<String, ?>> validations) {
        for (Map<String, ?> validation : validations) {
            if (validation.size() != 1)
                throw new IllegalEntityDefinition("illegal validation definition (map must have one entry)");
        }
        this.validations = validations;
    }

    public BiMap<String, EntityIndex> getIndexes() {
        return indexes;
    }

    public void setIndexes(Map<String, List<List<String>>> indexes) {
        this.indexes = HashBiMap.create(indexes.size());
        for (Map.Entry<String, List<List<String>>> entry : indexes.entrySet()) {
            EntityIndex index = EntityIndex.create(entry.getValue());
            this.indexes.put(entry.getKey(), index);
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
        return Util.toJsonString(this);
    }

    private UUID uuid;
    private Map<String, Attribute> attributes = Collections.emptyMap();
    private List<Map<String, ?>> validations = Collections.emptyList();
    private BiMap<String, EntityIndex> indexes = ImmutableBiMap.of();

    private Entity() {}
}
