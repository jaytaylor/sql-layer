/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.physicaloperator;

import com.akiban.qp.rowtype.RowType;

import java.util.List;

public abstract class UsablePhysicalOperator extends PhysicalOperator {

    // PhysicalOperator interface (promoted visibility)

    @Override
    public abstract Cursor cursor(StoreAdapter adapter);

    // UsablePhysicalOperator interface

    public static UsablePhysicalOperator of(PhysicalOperator root) {
        return new DefaultUsablePhysicalOperator(root);
    }

    // inner classes

    private static class DefaultUsablePhysicalOperator extends UsablePhysicalOperator {

        // UsablePhysicalOperator interface

        @Override
        public Cursor cursor(StoreAdapter adapter) {
            return new TopLevelWrappingCursor(root.cursor(adapter));
        }

        // PhysicalOperator interface

        @Override
        public String describePlan() {
            return super.describePlan();
        }

        @Override
        public boolean cursorAbilitiesInclude(CursorAbility ability) {
            return super.cursorAbilitiesInclude(ability);
        }

        @Override
        public List<PhysicalOperator> getInputOperators() {
            return super.getInputOperators();
        }

        @Override
        public RowType rowType() {
            return super.rowType();
        }

        // Object interface

        @Override
        public String toString() {
            return super.toString();
        }

        // DefaultUsablePhysicalOperator interface

        private DefaultUsablePhysicalOperator(PhysicalOperator root) {
            this.root = root;
        }

        // object state

        private final PhysicalOperator root;
    }

}
