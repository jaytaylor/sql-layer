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

package com.akiban.server.t3expressions;

import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.util.Strings;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

abstract class OverloadsFolder<T> {

    protected abstract T attribute(TInputSet inputSet);
    protected abstract T foldOne(T accumulated, T input);

    public Result<T> fold(Collection<? extends TValidatedOverload> overloads) {
        int nFinites = 0;
        boolean anyVarargs = false;
        for (TValidatedOverload overload : overloads) {
            int nArgs = overload.positionalInputs();
            nFinites = Math.max(nFinites, nArgs);
            if (overload.isVararg())
                anyVarargs = true;
        }

        List<T> finitesList = new ArrayList<T>(nFinites);
        for (int pos = 0; pos < nFinites; ++pos) {
            T result = foldBy(overloads, new FoldByPositionalArity(pos));
            finitesList.add(result);
        }

        T infiniteArityElement;
        boolean hasInfiniteArityElement;
        if (anyVarargs) {
            infiniteArityElement = foldBy(overloads, foldByVarags);
            hasInfiniteArityElement = true;
        }
        else {
            infiniteArityElement = null;
            hasInfiniteArityElement = false;
        }
        return new Result<T>(finitesList, infiniteArityElement, hasInfiniteArityElement);
    }

    private T foldBy(Collection<? extends TValidatedOverload> overloads, Function<TValidatedOverload, TInputSet> f) {
        T result = null;
        boolean seenOne = false;
        for (TValidatedOverload overload : overloads) {
            TInputSet inputSet = f.apply(overload);
            if (inputSet != null) {
                T attribute = attribute(inputSet);
                if (seenOne) {
                    result = foldOne(result, attribute);
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
            if (pos < input.positionalInputs())
                return input.inputSetAt(pos);
            else if (input.isVararg())
                return input.varargInputSet();
            else
                return null;
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

        public T infiniteArityElement() {
            if (!hasInfiniteArityElement)
                throw new IllegalStateException("no infinite arity element");
            return infiniteArityElement;
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
            return new Result<M>(mappedFinites, mappedInfinite, hasInfiniteArityElement);
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
