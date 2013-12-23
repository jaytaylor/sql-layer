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

package com.foundationdb.server.types.service;

import com.foundationdb.server.types.InputSetFlags;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.texpressions.TValidatedOverload;
import com.foundationdb.util.Strings;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

abstract class OverloadsFolder {

    protected abstract TClass foldOne(TClass accumulated, TClass input);

    public Result<TClass> fold(Collection<? extends TValidatedOverload> overloads) {
        int nFinites = 0;
        boolean anyVarargs = false;
        for (TValidatedOverload overload : overloads) {
            int nArgs = overload.positionalInputs();
            nFinites = Math.max(nFinites, nArgs);
            if (overload.isVararg())
                anyVarargs = true;
        }

        List<TClass> finitesList = new ArrayList<>(nFinites);
        for (int pos = 0; pos < nFinites; ++pos) {
            TClass result = foldBy(overloads, new FoldByPositionalArity(pos));
            finitesList.add(result);
        }

        TClass infiniteArityElement;
        boolean hasInfiniteArityElement;
        if (anyVarargs) {
            infiniteArityElement = foldBy(overloads, foldByVarags);
            hasInfiniteArityElement = true;
        }
        else {
            infiniteArityElement = null;
            hasInfiniteArityElement = false;
        }
        return new Result<>(finitesList, infiniteArityElement, hasInfiniteArityElement);
    }

    private TClass foldBy(Collection<? extends TValidatedOverload> overloads, Function<TValidatedOverload, TInputSet> f) {
        TClass result = null;
        boolean seenOne = false;
        for (TValidatedOverload overload : overloads) {
            TInputSet inputSet = f.apply(overload);
            if (inputSet != null) {
                TClass attribute = inputSet.targetType();
                if (seenOne) {
                    if (attribute != null) {
                        result = (result == null) ? attribute : foldOne(result, attribute);
                    }
                }
                else {
                    result = attribute;
                    seenOne = true;
                }
            }
        }
        assert seenOne;
        return result;
    }

    private static class FoldByPositionalArity implements Function<TValidatedOverload, TInputSet> {

        @Override
        public TInputSet apply(TValidatedOverload input) {
            return pos < input.positionalInputs()
                    ? input.inputSetAt(pos)
                    : input.varargInputSet();
        }

        public FoldByPositionalArity(int pos) {
            this.pos = pos;
        }

        private int pos;
    }

    private static final Function<TValidatedOverload, TInputSet> foldByVarags
            = new Function<TValidatedOverload, TInputSet>() {
        @Override
        public TInputSet apply(TValidatedOverload input) {
            return input.varargInputSet();
        }
    };

    static class Result<T> {

        public List<T> finiteArityList() {
            return finiteArityList;
        }

        public T infiniteArityElement(T ifNone) {
            return hasInfiniteArityElement ? infiniteArityElement : ifNone;
        }

        public T at(int i, T ifUndefined) {
            if (i < finiteArityList.size()) {
                return finiteArityList.get(i);
            }
            else {
                return hasInfiniteArityElement ? infiniteArityElement : ifUndefined;
            }
        }

        public <M> Result<M> transform(Function<? super T, ? extends M> mapFunction) {
            List<M> mappedFinites = Lists.transform(finiteArityList, mapFunction);
            M mappedInfinite = hasInfiniteArityElement ? mapFunction.apply(infiniteArityElement) : null;
            return new Result<>(mappedFinites, mappedInfinite, hasInfiniteArityElement);
        }

        public InputSetFlags toInputSetFlags(Predicate<? super T> predicate) {
            boolean[] finites = new boolean[finiteArityList.size()];
            for (int i = 0; i < finites.length; ++i) {
                finites[i] = predicate.apply(finiteArityList.get(i));
            }
            boolean infinite = predicate.apply(infiniteArityElement);
            return new InputSetFlags(finites, infinite);
        }

        // Object interface
        @Override
        public String toString() {
            String finites = Strings.join(finiteArityList, ", ");
            return hasInfiniteArityElement
                    ? (finites + ", " + infiniteArityElement + "...")
                    : finites;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Result result = (Result) o;
            return finiteArityList.equals(result.finiteArityList)
                    && infiniteArityElement.equals(result.infiniteArityElement);
        }

        @Override
        public int hashCode() {
            int result = finiteArityList.hashCode();
            result = 31 * result + infiniteArityElement.hashCode();
            return result;
        }

        public Result(List<T> finiteArityList, T infiniteArityElement, boolean hasInfiniteArityElement) {
            assert finiteArityList != null;
            this.finiteArityList = finiteArityList;
            this.infiniteArityElement = infiniteArityElement;
            this.hasInfiniteArityElement = hasInfiniteArityElement;
        }

        private final List<T> finiteArityList;
        private final T infiniteArityElement;
        private final boolean hasInfiniteArityElement;
    }
}
