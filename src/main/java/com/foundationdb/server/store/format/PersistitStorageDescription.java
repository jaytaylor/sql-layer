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

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.PersistitProtobuf;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.error.RowDataCorruptionException;
import com.foundationdb.server.rowdata.CorruptRowDataException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.TreeLink;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.store.StoreStorageDescription;
import com.persistit.Exchange;

import com.persistit.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Storage in a persistit volume tree. 
 * Goes to a lot of trouble to arrange for the name of the tree to be
 * meaningful, while still unique.
*/
public class PersistitStorageDescription extends StoreStorageDescription<PersistitStore,Exchange> implements TreeLink
{
    private static final Logger LOG = LoggerFactory.getLogger(PersistitStorageDescription.class);

    private String treeName;
    private Tree treeCache;

    public PersistitStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public PersistitStorageDescription(HasStorage forObject, String treeName, String storageFormat) {
        super(forObject, storageFormat);
        this.treeName = treeName;
    }

    public PersistitStorageDescription(HasStorage forObject, PersistitStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.treeName = other.treeName;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new PersistitStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        return new PersistitStorageDescription(forObject, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(PersistitProtobuf.treeName, treeName);
        writeUnknownFields(builder);
    }

    @Override
    public String getTreeName() {
        return treeName;
    }

    protected void setTreeName(String treeName) {
        this.treeName = treeName;
    }

    @Override
    public Tree getTreeCache() {
        return treeCache;
    }

    @Override
    public void setTreeCache(Tree cache) {
        this.treeCache = cache;
    }

    @Override
    public Object getUniqueKey() {
        return treeName;
    }

    @Override
    public String getNameString() {
        return treeName;
    }

    @Override
    public void validate(AISValidationOutput output) {
        if (treeName == null) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is missing tree name")));
        }
    }

    @Override
    public void packRowData(PersistitStore store, Session session,
                            Exchange exchange, RowData rowData) {
        exchange.getValue().directPut(store.getRowDataValueCoder(), rowData, null);
    }

    @Override
    public void expandRowData(PersistitStore store, Session session,
                              Exchange exchange, RowData rowData) {
        try {
            exchange.getValue().directGet(store.getRowDataValueCoder(), rowData, RowData.class, null);
        }
        catch (CorruptRowDataException ex) {
            LOG.error("Corrupt RowData at key {}: {}", exchange.getKey(), ex.getMessage());
            throw new RowDataCorruptionException(exchange.getKey());
        }
    }

}
