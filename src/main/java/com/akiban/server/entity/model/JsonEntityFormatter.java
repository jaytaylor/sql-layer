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
