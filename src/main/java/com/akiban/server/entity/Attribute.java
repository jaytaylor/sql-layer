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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public final class Attribute {

    void validate(Set<? super UUID> uuids) {
        if (attributeType == null)
            throw new IllegalEntityDefinition("no attribute type set (needs to be scalar or collection)");
        assert uuid != null : "no uuid set";
        if (!uuids.add(uuid))
            throw new IllegalEntityDefinition("duplicate uuid found: " + uuid);
        if (attributeType == AttributeType.SCALAR) {
            if (type == null)
                throw new IllegalEntityDefinition("no type set for scalar");
            if (attributes != null)
                throw new IllegalEntityDefinition("attributes can't be set for scalar");
        }
        else if (attributeType == AttributeType.COLLECTION) {
            if (type != null)
                throw new IllegalEntityDefinition("type can't be set for collection");
            if (attributes == null)
                throw new IllegalEntityDefinition("no attributes set for collection");
            for (Attribute attribute : attributes.values())
                attribute.validate(uuids);
        }
        else {
            throw new AssertionError("unknown attribute type: " + attributeType);
        }
    }

    public UUID getUUID() {
        return uuid;
    }

    public AttributeType getAttributeType() {
        return attributeType;
    }

    // scalar fields

    public void setScalar(String uuid) {
        if (attributeType != null)
            throw new IllegalEntityDefinition("'scalar' field not allowed; attribute is already a " + attributeType);
        this.uuid = Util.parseUUID(uuid);
        attributeType = AttributeType.SCALAR;
    }

    public void setAttributeType(AttributeType attributeType) {
        this.attributeType = attributeType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, ?> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, ?> properties) {
        this.properties = properties;
    }

    public List<Map<String, ?>> getValidation() {
        return validations;
    }

    public void setValidation(List<Map<String, ?>> validations) {
        for (Map<String, ?> validation : validations) {
            if (validation.size() != 1)
                throw new IllegalEntityDefinition("illegal validation definition");
        }
        this.validations = validations;
    }

    public boolean isId() {
        return isId;
    }

    public void setId(boolean id) {
        isId = id;
    }

    // collection fields

    public void setCollection(String uuid) {
        if (attributeType != null)
            throw new IllegalEntityDefinition(
                    "'collection' field not allowed; attribute is already a " + attributeType);
        this.uuid = Util.parseUUID(uuid);
        attributeType = AttributeType.COLLECTION;
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Attribute> attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Attribute attribute = (Attribute) o;
        if (attributeType == null)
            return  attribute.attributeType == null;
        if (!uuid.equals(attribute.uuid))
            return  false;
        if (attributeType == AttributeType.SCALAR) {
            return (isId == attribute.isId)
                    && Objects.equals(type, attribute.type)
                    && Objects.equals(properties, attribute.properties)
                    && Objects.equals(validations, attribute.validations);
        }
        else if (attributeType == AttributeType.COLLECTION) {
            return Objects.equals(attributes, attribute.attributes);
        }
        else {
            throw new AssertionError("unknown attribute type: " + attributeType);
        }
    }

    @Override
    public int hashCode() {
        if (attributeType == null)
            return 0;
        int result = attributeType.hashCode();
        result = 31 * result + (uuid != null ? uuid.hashCode() : 0);
        if (attributeType == AttributeType.SCALAR) {
            result = 31 * result + (type != null ? type.hashCode() : 0);
            result = 31 * result + properties.hashCode();
            result = 31 * result + validations.hashCode();
            result = 31 * result + (isId ? 1 : 0);
        }
        else if (attributeType == AttributeType.COLLECTION) {
            result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        }
        else {
            throw new AssertionError("unknown attribute type: " + attributeType);
        }
        return result;
    }

    @Override
    public String toString() {
        return Util.toJsonString(this);
    }

    // common fields
    private UUID uuid;
    private AttributeType attributeType;

    // scalar fields
    private String type;
    private Map<String, ?> properties = Collections.emptyMap();
    private List<Map<String, ?>> validations = Collections.emptyList();
    private boolean isId;

    // collection fields
    private Map<String, Attribute> attributes;

    private Attribute() {}

    public enum AttributeType {
        SCALAR,
        COLLECTION
    }
}
