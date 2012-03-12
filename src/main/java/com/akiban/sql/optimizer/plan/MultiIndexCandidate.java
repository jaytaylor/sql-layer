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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.util.Equality;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p>A struct + builder for the combination of an Index and a set of conditions against which that index is pegged.
 * The conditions are generic for ease of testing, and this class defines an abstract method for answering whether
 * a given condition matches a given column; the rest of the logic doesn't depend on what the condition is.</p>
 *
 * <p>The expectation is that there are two subclasses of this: one for unit testing, and one for production.</p>
 *
 * <p>A pegged condition is one which has been assigned to a column of the MultiIndexCandidate's Index; it will
 * eventually turn into a predicate on the index scan. For instance, say we had an index
 * <code>c_idx ON customers(name, dob)</code>. The index's full columns list is <code>[name, dob, cid]</code>, and each
 * one of those columns represents a slot into which a condition can be pegged. For instance, given the conditions
 * <code>{name="Bob", name="Mary", dob="1971-02-03"}</code>, we could expect two MultiIndexCandidates:</p>
 * <table border="1">
 *     <tr><th>name</th><th>dob</th><th>cid</th></tr>
 *     <tr><td>pegged to "Bob"</td><td>pegged to "1971-02-03"</td><td>unpegged</td></tr>
 *     <tr><td>pegged to "Mary"</td><td>pegged to "1971-02-03"</td><td>unpegged</td></tr>
 * </table>
 * @param <C> the condition type.
 */
public class MultiIndexCandidate<C> {
    private Index index;
    private List<C> pegged;

    public MultiIndexCandidate(Index index) {
        this.index = index;
        pegged = new ArrayList<C>(index.getKeyColumns().size());
    }

    public boolean anyPegged() {
        return ! pegged.isEmpty();
    }

    public Column getNextFreeColumn() {
        int nextFreeIndex = pegged.size();
        List<IndexColumn> columns = index.getKeyColumns();
        if (nextFreeIndex >= columns.size()) {
            nextFreeIndex -= columns.size();
            columns = index.getValueColumns();
        }
        return nextFreeIndex >= columns.size()
            ? null
            : columns.get(nextFreeIndex).getColumn();
    }
    
    public void peg(C condition) {
        pegged.add(condition);
    }
    
    public List<C> getPegged() {
        return pegged;
    }

    public Index getIndex() {
        return index;
    }

    public List<Column> getUnpeggedColumns() {
        List<IndexColumn> keyColumns = index.getKeyColumns();
        List<IndexColumn> valueColumns = index.getValueColumns();

        int startAt = pegged.size();
        int endAt = keyColumns.size() + valueColumns.size();
        if (endAt - startAt == 0)
            return Collections.emptyList();

        List<Column> results = new ArrayList<Column>(endAt - startAt);
        int offset = 0;
        List<IndexColumn> indexColumnList = keyColumns;
        for (int i = startAt; i < endAt; ++i) {
            if (indexColumnList == keyColumns && i >= keyColumns.size()) {
                offset = keyColumns.size();
                indexColumnList = valueColumns;
            }
            Column column = indexColumnList.get(i - offset).getColumn();
            results.add(column);
        }
        return results;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append(index).append(" pegged to ");
        if (pegged == null || pegged.isEmpty())
            sb.append("nothing");
        else
            sb.append(pegged);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MultiIndexCandidate<?> other = (MultiIndexCandidate<?>) o;

        return Equality.areEqual(index, other.index) && Equality.areEqual(pegged, other.pegged);

    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + (pegged != null ? pegged.hashCode() : 0);
        return result;
    }
}
