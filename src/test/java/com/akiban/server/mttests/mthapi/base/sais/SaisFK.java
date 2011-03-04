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

package com.akiban.server.mttests.mthapi.base.sais;

import com.akiban.util.ArgumentValidation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class SaisFK {
    private final SaisTable child;
    private final Map<String,String> fkFields;

    SaisFK(SaisTable child, Map<String, String> fkFields) {
        ArgumentValidation.isGTE("fkFields", fkFields.size(), 1);
        if (!child.getFields().containsAll(fkFields.values())) {
            throw new IllegalArgumentException("child doesn't contain all FK columns");
        }
        this.child = child;
        this.fkFields = Collections.unmodifiableMap( new HashMap<String, String>(fkFields) );
    }

    public SaisTable getChild() {
        return child;
    }

    public Map<String, String> getFkFields() {
        return fkFields;
    }

    @Override
    public String toString() {
        return String.format("FK[to %s]", getFkFields());
    }
}
