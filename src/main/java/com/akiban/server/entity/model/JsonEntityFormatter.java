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

import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;

public final class JsonEntityFormatter implements EntityVisitor<IOException> {
    @Override
    public void enterTopEntity(Entity entity) throws IOException {
        startEntityObject(entity);
    }

    @Override
    public void leaveTopEntity() throws IOException {
        json.writeEndObject();
    }

    @Override
    public void enterCollections() throws IOException {
        json.writeArrayFieldStart("collections");
    }

    @Override
    public void enterCollection(EntityCollection collection) throws IOException {
        startEntityObject(collection);
        json.writeObjectField("grouping_fields", collection.getGroupingFields());
    }

    @Override
    public void leaveCollection() throws IOException {
        json.writeEndObject();
    }

    @Override
    public void leaveCollections() throws IOException {
        json.writeEndArray();
    }

    private void writeElement(EntityElement element) throws IOException {
        json.writeStringField("name", element.getName());
        json.writeStringField("uuid", element.getUuid().toString());
    }

    private void startEntityObject(Entity entity) throws IOException {
        json.writeStartObject();
        writeElement(entity);
        json.writeArrayFieldStart("fields"); {
            for (EntityField field : entity.getFields())
                json.writeObject(field);
        } json.writeEndArray();
        if (!entity.getIdentifying().isEmpty())
            json.writeObjectField("identifying", entity.getIdentifying());
        if (!entity.getIndexes().isEmpty())
            json.writeObjectField("indexes", entity.getIndexes());
        if (!entity.getValidations().isEmpty())
            json.writeObjectField("validations", entity.getValidations());
    }


    //    @Override
//    public void visitEntity(String name, Entity entity) throws IOException {
//        json.writeObjectFieldStart(name);
//        json.writeStringField("entity", entity.uuid().toString());
//        json.writeObjectFieldStart("attributes");
//    }
//
//    @Override
//    public void leaveTopEntity() throws IOException {
//        json.writeEndObject();
//    }
//
//    @Override
//    public void visitScalar(String name, Attribute scalar) throws IOException {
//        json.writeObjectFieldStart(name);
//        json.writeStringField("scalar", scalar.getUUID().toString());
//        json.writeStringField("type", scalar.getType());
//        if (!scalar.getProperties().isEmpty())
//            json.writeObjectField("properties", scalar.getProperties());
//        writeValidations(scalar.getValidation());
//        if (scalar.isSpinal())
//            json.writeNumberField("spinal_pos", scalar.getSpinePos());
//        json.writeEndObject();
//    }
//
//    @Override
//    public void visitCollection(String name, Attribute collection) throws IOException {
//        json.writeObjectFieldStart(name);
//        json.writeStringField("collection", collection.getUUID().toString());
//        json.writeObjectFieldStart("attributes");
//    }
//
//    @Override
//    public void leaveCollection() throws IOException {
//        json.writeEndObject(); // "attributes" object
//        json.writeEndObject(); // the collection itself
//    }
//
//    @Override
//    public void leaveEntityAttributes() throws IOException {
//        json.writeEndObject();
//    }
//
//    @Override
//    public void visitEntityValidations(Set<Validation> validations) throws IOException {
//        writeValidations(validations);
//    }
//
//    @Override
//    public void visitIndexes(BiMap<String, EntityIndex> indexes) throws IOException {
//        if (!indexes.isEmpty()) {
//            json.writeObjectFieldStart("indexes");
//            for (Map.Entry<String, EntityIndex> indexEntry : indexes.entrySet()) {
//                json.writeArrayFieldStart(indexEntry.getKey());
//                for (IndexField column : indexEntry.getValue().getFields()) {
//                    json.writeStartArray();
//                    json.writeString(column.getTable());
//                    json.writeString(column.getColumn());
//                    json.writeEndArray();
//                }
//                json.writeEndArray();
//            }
//            json.writeEndObject();
//        }
//    }
//
//    private void writeValidations(Collection<Validation> validations) throws IOException {
//        if (!validations.isEmpty()) {
//            json.writeArrayFieldStart("validation");
//            for (Validation validation : validations) {
//                json.writeStartObject();
//                json.writeObjectField(validation.getName(), validation.getValue());
//                json.writeEndObject();
//            }
//            json.writeEndArray();
//        }
//    }

    public JsonEntityFormatter(JsonGenerator json, Void doWeNeedThisClass) {
        this.json = json;
    }

    private final JsonGenerator json;
}
