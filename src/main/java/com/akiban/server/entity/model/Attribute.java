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

package com.akiban.server.entity.model;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

public final class Attribute {

    public UUID getUUID() {
        return uuid;
    }

    void setUuid(UUID uuid) {
        if (this.uuid != null)
            throw new IllegalStateException("uuid already set: " + this.uuid);
        this.uuid = uuid;
    }

    public AttributeType getAttributeType() {
        return attributeType;
    }

    // scalar fields

    @SuppressWarnings("unused")
    void setScalar(String uuid) {
        if (attributeType != null)
            throw new IllegalEntityDefinition("'scalar' field not allowed; attribute is already a " + attributeType);
        this.uuid = Util.parseUUID(uuid);
        attributeType = AttributeType.SCALAR;
    }

    @SuppressWarnings("unused")
    void setAttributeType(AttributeType attributeType) {
        this.attributeType = attributeType;
    }

    public String getType() {
        return type;
    }

    @SuppressWarnings("unused")
    void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @SuppressWarnings("unused")
    void setProperties(Map<String, ?> properties) {
        this.properties = Collections.unmodifiableMap(properties);
    }

    public Collection<Validation> getValidation() {
        return validations;
    }

    @SuppressWarnings("unused")
    void setValidation(List<Map<String, ?>> validations) {
        this.validations = Validation.createValidations(validations);
    }

    public boolean isSpinal() {
        return spine >= 0;
    }

    public int getSpinePos() {
        return spine;
    }

    @SuppressWarnings("unused")
    @JsonProperty("spinal_pos")
    public void setSpinalPos(int spine) {
        if (spine < 0)
            throw new IllegalEntityDefinition("spine may not be negative");
        this.spine = spine;
    }

    // collection fields

    @SuppressWarnings("unused")
    void setCollection(String uuid) {
        if (attributeType != null)
            throw new IllegalEntityDefinition(
                    "'collection' field not allowed; attribute is already a " + attributeType);
        this.uuid = Util.parseUUID(uuid);
        attributeType = AttributeType.COLLECTION;
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    @SuppressWarnings("unused")
    void setAttributes(Map<String, Attribute> attributes) {
        this.attributes = Collections.unmodifiableMap(new TreeMap<>(attributes));
    }

    @Override
    public String toString() {
        return String.format("%s {%s}", attributeType.name().toLowerCase(), uuid);
    }

    // common fields
    private UUID uuid;
    private AttributeType attributeType;

    // scalar fields
    private String type;
    private Map<String, Object> properties = Collections.emptyMap();
    private Set<Validation> validations = Collections.emptySet();
    private int spine = -1;

    // collection fields
    private Map<String, Attribute> attributes;

    public static Attribute modifiableScalar(UUID uuid, String type) {
        Attribute scalar = new Attribute();
        scalar.uuid = uuid;
        scalar.attributeType = AttributeType.SCALAR;
        scalar.type = type;
        scalar.properties = new HashMap<>();
        scalar.validations = new TreeSet<>();
        return scalar;
    }

    public static Attribute modifiableCollection(UUID uuid) {
        Attribute collection = new Attribute();
        collection.uuid = uuid;
        collection.attributeType = AttributeType.COLLECTION;
        collection.attributes = new TreeMap<>();
        return collection;
    }

    private Attribute() {}

    public <E extends Exception> void accept(String myName, EntityVisitor<E> visitor) throws E {
        if (attributeType == null)
            throw new IllegalEntityDefinition("attribute " + myName + " has no attribute type (scalar or collection)");
        if (attributeType == AttributeType.SCALAR) {
            visitor.visitScalar(myName, this);
        }
        else if (attributeType == AttributeType.COLLECTION) {
            visitor.visitCollection(myName, this);
            for(Map.Entry<String, Attribute> child : attributes.entrySet()) {
                child.getValue().accept(child.getKey(), visitor);
            }
            visitor.leaveCollection();
        }
        else {
            assert false : "unknown attribute type: " + attributeType;
        }
    }

    public enum AttributeType {
        SCALAR,
        COLLECTION
    }
}
