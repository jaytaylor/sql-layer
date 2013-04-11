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

package com.akiban.server.entity.changes;

import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;

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
