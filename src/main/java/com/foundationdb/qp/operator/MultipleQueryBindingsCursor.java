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

package com.foundationdb.qp.operator;

import java.util.ArrayList;
import java.util.List;

/**
 * A generator of {@link QueryBindingsCursor}s from a common source.
 */
public class MultipleQueryBindingsCursor implements QueryBindingsCursor
{
    private final QueryBindingsCursor input;
    private final List<QueryBindings> buffer = new ArrayList<QueryBindings>();
    private final List<SubCursor> cursors = new ArrayList<SubCursor>();
    private boolean exhausted;
    private int offset;
    
    public MultipleQueryBindingsCursor(QueryBindingsCursor input) {
        this.input = input;
        newCursor();
    }

    @Override
    public void openBindings() {
        input.openBindings();
        exhausted = false;
        buffer.clear();
        offset = 0;
        cursors.get(0).openBindings();
        for (int i = 1; i < cursors.size(); i++) {
            cursors.get(i).closeBindings();
        }
    }

    @Override
    public QueryBindings nextBindings() {
        return cursors.get(0).nextBindings();
    }

    @Override
    public void closeBindings() {
        cursors.get(0).closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        cursors.get(0).cancelBindings(bindings);
        input.cancelBindings(bindings);
    }

    public QueryBindingsCursor newCursor() {
        assert (offset == 0);
        SubCursor cursor = new SubCursor();
        cursors.add(cursor);
        return cursor;
    }

    protected void shrink() {
        int minIndex = cursors.get(0).index;
        for (int i = 1; i < cursors.size(); i++) {
            SubCursor cursor = cursors.get(i);
            if (!cursor.open) continue;
            if (minIndex > cursor.index) {
                minIndex = cursor.index;
            }
        }
        while (offset < minIndex) {
            buffer.remove(0);
            offset++;
        }
    }

    class SubCursor implements QueryBindingsCursor {
        boolean open;
        int index;

        @Override
        public void openBindings() {
            assert (offset == 0);
            open = true;
            index = 0;
        }

        @Override
        public QueryBindings nextBindings() {
            assert (open);
            assert (index >= offset);
            while (index - offset >= buffer.size()) {
                if (exhausted) {
                    return null;
                }
                else {
                    QueryBindings bindings = input.nextBindings();
                    if (bindings == null) {
                        exhausted = true;
                        return null;
                    }
                    buffer.add(bindings);
                }
            }
            QueryBindings bindings = buffer.get(index - offset);
            index++;
            shrink();
            return bindings;
        }

        @Override
        public void closeBindings() {
            open = false;
        }

        @Override
        public void cancelBindings(QueryBindings ancestor) {
            while (index - offset < buffer.size()) {
                QueryBindings bindings = buffer.get(index - offset);
                if (bindings.isAncestor(ancestor)) {
                    index++;
                }
                else {
                    break;
                }
            }
            shrink();
        }
    }
}
