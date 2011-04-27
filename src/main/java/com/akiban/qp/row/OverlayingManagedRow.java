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

package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;

public final class OverlayingManagedRow extends RowBase {

    public interface OverlayingManagedRowBuilder {
        OverlayingManagedRowBuilder overlay(int index, Object object);
        OverlayingManagedRow done();
    }

    public static OverlayingManagedRowBuilder buildFrom(ManagedRow underlying) {
        return new DefaultOverlayingManagedRowBuilder(underlying);
    }

    private static final Object UNDEF = new Object() {
        @Override
        public String toString() {
            return "UNDEF";
        }
    };
    private final ManagedRow underlying;
    private final Object[] overlays;

    private OverlayingManagedRow(ManagedRow underlying, Object[] overlays) {
        this.underlying = underlying;
        this.overlays = overlays;
    }

    @Override
    public RowType rowType() {
        return underlying.rowType();
    }

    @Override
    public Object field(int i) {
        return overlays[i] == UNDEF ? underlying.field(i) : overlays[i];
    }

    @Override
    public HKey hKey() {
        return underlying.hKey();
    }

    private static class DefaultOverlayingManagedRowBuilder implements OverlayingManagedRowBuilder {
        private final ManagedRow underlying;
        private final Object[] overlays;

        private DefaultOverlayingManagedRowBuilder(ManagedRow underlying) {
            this.underlying = underlying;
            overlays = new Object[underlying.rowType().nFields()];
            for (int i=0; i < overlays.length; ++i) {
                overlays[i] = UNDEF;
            }
        }

        @Override
        public OverlayingManagedRowBuilder overlay(int index, Object object) {
            overlays[index] = object;
            return this;
        }

        @Override
        public OverlayingManagedRow done() {
            return new OverlayingManagedRow(underlying, overlays);
        }
    }
}
