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

    void assignUuid() {
        assert uuid == null : uuid;
        uuid = UUID.randomUUID();
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
