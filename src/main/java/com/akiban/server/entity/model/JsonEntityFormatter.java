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
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class JsonEntityFormatter implements EntityVisitor<IOException> {

    @Override
    public void visitEntity(String name, Entity entity) throws IOException {
        json.writeObjectFieldStart(name);
        json.writeStringField("entity", entity.uuid().toString());
        json.writeObjectFieldStart("attributes");
    }

    @Override
    public void leaveEntity() throws IOException {
        json.writeEndObject();
    }

    @Override
    public void visitScalar(String name, Attribute scalar) throws IOException {
        json.writeObjectFieldStart(name);
        json.writeStringField("scalar", scalar.getUUID().toString());
        json.writeStringField("type", scalar.getType());
        if (!scalar.getProperties().isEmpty())
            json.writeObjectField("properties", scalar.getProperties());
        writeValidations(scalar.getValidation());
        if (scalar.isSpinal())
            json.writeNumberField("spinal_pos", scalar.getSpinePos());
        json.writeEndObject();
    }

    @Override
    public void visitCollection(String name, Attribute collection) throws IOException {
        json.writeObjectFieldStart(name);
        json.writeStringField("collection", collection.getUUID().toString());
        json.writeObjectFieldStart("attributes");
    }

    @Override
    public void leaveCollection() throws IOException {
        json.writeEndObject(); // "attributes" object
        json.writeEndObject(); // the collection itself
    }

    @Override
    public void leaveEntityAttributes() throws IOException {
        json.writeEndObject();
    }

    @Override
    public void visitEntityValidations(Set<Validation> validations) throws IOException {
        writeValidations(validations);
    }

    @Override
    public void visitIndexes(BiMap<String, EntityIndex> indexes) throws IOException {
        if (!indexes.isEmpty()) {
            json.writeObjectFieldStart("indexes");
            for (Map.Entry<String, EntityIndex> indexEntry : indexes.entrySet()) {
                json.writeArrayFieldStart(indexEntry.getKey());
                for (EntityColumn column : indexEntry.getValue().getColumns()) {
                    json.writeStartArray();
                    json.writeString(column.getTable());
                    json.writeString(column.getColumn());
                    json.writeEndArray();
                }
                json.writeEndArray();
            }
            json.writeEndObject();
        }
    }

    private void writeValidations(Collection<Validation> validations) throws IOException {
        if (!validations.isEmpty()) {
            json.writeArrayFieldStart("validation");
            for (Validation validation : validations) {
                json.writeStartObject();
                json.writeObjectField(validation.getName(), validation.getValue());
                json.writeEndObject();
            }
            json.writeEndArray();
        }
    }

    public JsonEntityFormatter(JsonGenerator json) {
        this.json = json;
    }

    private final JsonGenerator json;
}
