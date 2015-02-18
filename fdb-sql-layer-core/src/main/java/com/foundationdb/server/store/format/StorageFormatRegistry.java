/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.store.format;

import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.qp.virtualadapter.VirtualScanFactory;
import com.foundationdb.sql.parser.StorageFormatNode;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/** A registry of mappings between DDL STORAGE_FORMAT clauses and
 * Protobuf extension fields and {@link StorageDescription} instances.
 */
public abstract class StorageFormatRegistry
{
    private final ConfigurationService configService;
    private String defaultIdentifier;

    public StorageFormatRegistry(String defaultIdentifier) {
        this.configService = null;
        this.defaultIdentifier = defaultIdentifier;
    }

    public StorageFormatRegistry(ConfigurationService configService) {
        this.configService = configService;
    }

    static class Format<T extends StorageDescription> implements Comparable<Format<?>> {
        final GeneratedMessage.GeneratedExtension<Storage,?> protobufExtension;
        final String sqlIdentifier;
        final Class<T> descriptionClass;
        final StorageFormat<T> storageFormat;

        public Format(GeneratedMessage.GeneratedExtension<Storage,?> protobufExtension, String sqlIdentifier, Class<T> descriptionClass, StorageFormat<T> storageFormat) {
            this.protobufExtension = protobufExtension;
            this.sqlIdentifier = sqlIdentifier;
            this.descriptionClass = descriptionClass;
            this.storageFormat = storageFormat;
        }

        public int compareTo(Format<?> other) {
            if (descriptionClass != other.descriptionClass) {
                // Do more specific class first.
                if (descriptionClass.isAssignableFrom(other.descriptionClass)) {
                    return +1;
                }
                else if (other.descriptionClass.isAssignableFrom(descriptionClass)) {
                    return -1;
                }
            }
            // Do higher field number first.
            return Integer.compare(other.protobufExtension.getDescriptor().getNumber(),
                    protobufExtension.getDescriptor().getNumber());
        }
    }
    private final ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();

    private final Collection<Format> formatsInOrder = new TreeSet<>();
    private final Map<Integer,Format> formatsByField = new TreeMap<>();
    private final Map<String,Format<?>> formatsByIdentifier = new TreeMap<>();
    private Constructor<? extends StorageDescription> defaultStorageConstructor;

    // The VirtualScanFactory itself cannot be serialized, so remember
    // it by group name and recover that way. Could remember a unique
    // id and actually write that, but sometimes the virtual table AIS
    // is actually written to disk.
    private final Map<TableName,VirtualScanFactory> virtualScanFactories = new HashMap<>();

    public void registerStandardFormats() {
        VirtualTableStorageFormat.register(this, virtualScanFactories);
        FullTextIndexFileStorageFormat.register(this);
        getDefaultDescriptionConstructor();
    }

    void getDefaultDescriptionConstructor() {
        Format<? extends StorageDescription> format = formatsByIdentifier.get(configService.getProperty("fdbsql.default_storage_format"));
        defaultIdentifier = configService.getProperty("fdbsql.default_storage_format");
        try {
            defaultStorageConstructor = format.descriptionClass.getConstructor(HasStorage.class, String.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public StorageDescription getDefaultStorageDescription(HasStorage object) {
        try {
            return defaultStorageConstructor.newInstance(object, defaultIdentifier);
        } catch (InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
    
    /** Return the Protbuf extension registry. */
    public ExtensionRegistry getExtensionRegistry() {
        return extensionRegistry;
    }

    /** Register a new {@link StorageFormat}.
     * @param protobufExtension the extension field that keys use of this format
     * @param sqlIdentifier the <code>STORAGE_FORMAT</code> identifier that keys use of this format or <code>null</code>
     * @param descriptionClass that specific class used to hold this format
     * @param storageFormat the mapping handler
     */
    public <T extends StorageDescription> void registerStorageFormat(GeneratedMessage.GeneratedExtension<Storage,?> protobufExtension, String sqlIdentifier, Class<T> descriptionClass, StorageFormat<T> storageFormat) {
        int fieldNumber = protobufExtension.getDescriptor().getNumber();
        if (formatsByField.containsKey(fieldNumber))
            throw new IllegalArgumentException("there is already a StorageFormat registered for field " + fieldNumber);
        if ((sqlIdentifier != null) &&
                formatsByIdentifier.containsKey(sqlIdentifier))
            throw new IllegalArgumentException("there is already a StorageFormat registered for STORAGE_FORMAT " + sqlIdentifier);
        if (!isDescriptionClassAllowed(descriptionClass)) {
            throw new IllegalArgumentException("description " + descriptionClass + " not allowed for " + getClass().getSimpleName());
        }
        extensionRegistry.add(protobufExtension);
        Format<T> format = new Format<T>(protobufExtension, sqlIdentifier, descriptionClass, storageFormat);
        formatsInOrder.add(format);
        formatsByField.put(fieldNumber, format);
        if (sqlIdentifier != null) {
            formatsByIdentifier.put(sqlIdentifier, format);
        }
    }

    /** Could this registry (and its associated store) support this class? */
    public boolean isDescriptionClassAllowed(Class<? extends StorageDescription> descriptionClass) {
        return (VirtualTableStorageDescription.class.isAssignableFrom(descriptionClass) ||
                FullTextIndexFileStorageDescription.class.isAssignableFrom(descriptionClass));
    }

    public void registerVirtualScanFactory(TableName name, VirtualScanFactory scanFactory) {
        virtualScanFactories.put(name, scanFactory);
    }

    public void unregisterVirtualScanFactory(TableName name) {
        virtualScanFactories.remove(name);
    }

    @SuppressWarnings("unchecked")
    public StorageDescription readProtobuf(Storage pbStorage, HasStorage forObject) {
        StorageDescription storageDescription = null;
        for (Format format : formatsInOrder) {
            if (pbStorage.hasExtension(format.protobufExtension)) {
                storageDescription = readProtobuf(format, pbStorage, forObject, storageDescription);
            }
        }
        if (!pbStorage.getUnknownFields().asMap().isEmpty()) {
            if (storageDescription == null) {
                storageDescription = new UnknownStorageDescription(forObject, defaultIdentifier);
            }
            storageDescription.setUnknownFields(pbStorage.getUnknownFields());
        }
        return storageDescription;
    }

    @SuppressWarnings("unchecked")
    protected <T extends StorageDescription> T readProtobuf(Format<T> format, Storage pbStorage, HasStorage forObject, StorageDescription storageDescription) {
        if ((storageDescription != null) &&
                !format.descriptionClass.isInstance(storageDescription)) {
            throw new IllegalStateException("incompatible storage format handlers: required " + format.descriptionClass.getName() + " but have " + storageDescription.getClass());
        }
        return format.storageFormat.readProtobuf(pbStorage, forObject, (T)storageDescription);
    }

    public StorageDescription parseSQL(StorageFormatNode node, HasStorage forObject) {
        Format<?> format = formatsByIdentifier.get(node.getFormat());
        if (format == null) {
            throw new UnsupportedSQLException("", node);
        }
        return format.storageFormat.parseSQL(node, forObject);
    }

    public void finishStorageDescription(HasStorage object, NameGenerator nameGenerator) {
        if (object.getStorageDescription() == null) {
            if (object instanceof Group) {
                VirtualScanFactory factory = virtualScanFactories.get(((Group)object).getName());
                if (factory != null) {
                    object.setStorageDescription(new VirtualTableStorageDescription(object, factory, VirtualTableStorageFormat.identifier));
                }
                else {
                    object.setStorageDescription(getDefaultStorageDescription(object));
                }
            }
            else if (object instanceof FullTextIndex) {
                File path = new File(nameGenerator.generateFullTextIndexPath((FullTextIndex)object));
                object.setStorageDescription(new FullTextIndexFileStorageDescription(object, path, FullTextIndexFileStorageFormat.identifier));
            }
            else { // Index or Sequence
                object.setStorageDescription(getDefaultStorageDescription(object));
            }
        }
    }
}
