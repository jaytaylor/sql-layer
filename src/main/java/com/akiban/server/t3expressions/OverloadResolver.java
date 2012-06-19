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

package com.akiban.server.t3expressions;

import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class OverloadResolver {
    TValidatedOverload get(String name, List<? extends TPreptimeValue> inputs) {
        Collection<? extends TValidatedOverload> namedOverloads = registry.get(name);
        if (namedOverloads.isEmpty())
            throw new NoSuchFunctionException(name);
        
        List<? extends TInputSet> inputSets;

        TClass pickingClass;
        TValidatedOverload resolvedOverload = null;

        int nInputs = inputs.size();
        if (namedOverloads.size() == 1) {
            resolvedOverload = namedOverloads.iterator().next();
            if (!resolvedOverload.coversNInputs(nInputs))
                throw new WrongExpressionArityException(resolvedOverload.positionalInputs(), nInputs);
            pickingClass = pickingClass(resolvedOverload);
        }
        if (resolvedOverload == null) {
            List<TValidatedOverload> candidates = new ArrayList<TValidatedOverload>(namedOverloads.size());
            for (TValidatedOverload overload : namedOverloads) {
                if ( overload.allowsInputs(inputs)) {
                    candidates.add(overload);
                }
            }
            // need to get picking classes, cull out based on specifics
        }
    }

    private TClass pickingClass(TValidatedOverload resolvedOverload) {
        throw new UnsupportedOperationException();
    }

    private T3ScalarsRegistery registry;
}
