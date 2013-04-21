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

import com.akiban.util.MessageDigestWriter;
import com.google.common.base.Function;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.io.Writer;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.akiban.util.JsonUtils.createJsonGenerator;
import static com.akiban.util.JsonUtils.readValue;

public final class Space {

    public static Space create(Reader reader, Function<String, UUID> uuidGenerator) throws IOException {
        Space result;
        try {
            result = readValue(reader, Space.class);
        } catch (JsonMappingException e) {
            if (e.getCause() instanceof IllegalEntityDefinition)
                throw (IllegalEntityDefinition) e.getCause();
            throw e;
        }
        result.visit(new Validator(uuidGenerator));
        return result;
    }

    public static Space readSpace(String fileName, Class<?> forClass, Function<String, UUID> uuidGenerator) {
        try (InputStream is = forClass.getResourceAsStream(fileName)) {
            if (is == null) {
                throw new RuntimeException("resource not found: " + fileName);
            }
            Reader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
            return create(reader, uuidGenerator);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void visit(EntityVisitor visitor) {
        for (Entity entity : entities) {
            entity.accept(visitor);
        }
    }

    public Collection<Entity> getEntities() {
        return entities;
    }

    void setEntities(Collection<Entity> entities) {
        this.entities = Collections.unmodifiableList(new ArrayList<>(entities));
    }

    @Override
    public String toString() {
        return entities.toString();
    }

    public static Space create(Collection<Entity> entities, Function<String, UUID> uuidGenerator) {
        Space space = new Space();
        space.setEntities(entities);
        space.visit(new Validator(uuidGenerator));
        return space;
    }

    public void generateJson(JsonGenerator json) throws IOException {
        json.writeStartObject();
        if (!entities.isEmpty()) {
            json.writeArrayFieldStart("entities");
            for (Entity entity : entities)
                json.writeObject(entity);
            json.writeEndArray();
        }
        json.writeEndObject();
    }

    public String toJson() {
        StringWriter writer = new StringWriter(); 
        jsonGenerate (writer);
        return writer.toString();
    }

    public String toHash () {
        try {
            StringWriter writer = new StringWriter();
            MessageDigestWriter md5writer = new MessageDigestWriter();
            JsonGenerator generator = createJsonGenerator(writer);
            generator.writeStartObject();
            generator.writeFieldName("hash");
            jsonGenerate (md5writer);
            generator.writeString(md5writer.getFormatMD5());
            generator.writeEndObject();
            generator.flush();
            writer.toString();
            return writer.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void jsonGenerate(Writer writer) {
        try {
            JsonGenerator generator = createJsonGenerator(writer);
            generateJson(generator);
            generator.flush();
            writer.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    Space() {}

    private List<Entity> entities = Collections.emptyList();

    private static class Validator extends AbstractEntityVisitor {

        @Override
        public void visitEntity(Entity container) {
            String name = container.getName();
            if (!collectionNames.add(name))
                throw new IllegalEntityDefinition("duplicate name within entity and collections: " + name);
            if (container.getFields() == null)
                throw new IllegalEntityDefinition("no fields defined for entity: " + container.getName());
        }

        @Override
        public void visitEntityElement(EntityElement element) {
            if (element.getUuid() == null)
                element.setUuid(uuidGenerator.apply(element.getName()));
            UUID uuid = element.getUuid();
            if (uuid == null)
                throw new IllegalEntityDefinition("no uuid specified");
            if (!uuids.add(uuid))
                throw new IllegalEntityDefinition("duplicate uuid found: " + uuid);
        }

        @Override
        protected void visitEntityField(EntityField field) {
            if (field.getType() == null)
                throw new IllegalEntityDefinition("no type set for field " + field.getName());
        }

        private Validator(Function<String, UUID> uuidGenerator) {
            this.uuidGenerator = uuidGenerator == null ? requireUUIDs : uuidGenerator;
        }

        private final Set<UUID> uuids = new HashSet<>();
        private final Set<String> collectionNames = new HashSet<>();
        private final Function<String, UUID> uuidGenerator;
    }


    public static final Function<String, UUID> randomUUIDs = new Function<String, UUID>() {
        @Override
        public UUID apply(String input) {
            return UUID.randomUUID();
        }
    };

    public static final Function<String, UUID> requireUUIDs = new Function<String, UUID>() {
        @Override
        public UUID apply(String input) {
            return null;
        }
    };
}
