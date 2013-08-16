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

package com.foundationdb.sql.optimizer.plan;

public final class OverlayedConditionsCount<C> implements ConditionsCount<C> {

    @Override
    public HowMany getCount(C condition) {
        HowMany oneCount = one.getCount(condition);
        switch (oneCount) {
        case NONE:
            return two.getCount(condition);
        case ONE:
            return two.getCount(condition) == HowMany.NONE ? HowMany.ONE : HowMany.MANY;
        case MANY:
            return HowMany.MANY;
        default:
            throw new AssertionError(oneCount.name());
        }
    }

    public OverlayedConditionsCount(ConditionsCount<? super C> one, ConditionsCount<? super C> two) {
        this.one = one;
        this.two = two;
    }

    private ConditionsCount<? super C> one;
    private ConditionsCount<? super C> two;
}
