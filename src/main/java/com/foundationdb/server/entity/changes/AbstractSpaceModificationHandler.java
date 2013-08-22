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

package com.foundationdb.server.entity.changes;

import com.foundationdb.server.entity.model.Entity;
import com.foundationdb.server.entity.model.EntityIndex;
import com.foundationdb.server.entity.model.Validation;

public abstract class AbstractSpaceModificationHandler implements SpaceModificationHandler {
    // This class is here to handle the common errors.
    // As long as all SpaceModificationHandler implementations extend from this class, and as long as none of those
    // subclasses invoke this.error(String), all errors should be in sync.
    // Please don't un-finalize any of the methods here. If you need to override them, just delete them from this
    // class altogether.

    @Override
    public final void identifyingFieldsChanged() {
        error("Can't change identifying fields");
    }

    @Override
    public final void groupingFieldsChanged() {
        error("Can't change grouping fields");
    }

    @Override
    public final void moveEntity(Entity oldParent, Entity newParent) {
        error("Can't move entities or collections");
    }

    @Override
    public final void addEntityValidation(Validation validation) {
        error("Adding entity validations is not yet supported: " + validation);
    }

    @Override
    public final void dropEntityValidation(Validation validation) {
        error("Dropping entity validations is not yet supported: " + validation);
    }

    @Override
    public final void renameIndex(EntityIndex index) {
// When we want to allow this, you can get the old/new names by doing something like this:
//        String oldName = oldEntity.getIndexes().inverse().get(index);
//        String newName = newEntity.getIndexes().inverse().get(index);
        error("Renaming index is not yet supported: " + index);
    }
}
