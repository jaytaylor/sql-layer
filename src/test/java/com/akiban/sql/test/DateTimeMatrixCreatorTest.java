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

package com.akiban.sql.test;

import org.junit.Assert;
import org.junit.Test;

public class DateTimeMatrixCreatorTest {

    @Test
    public void test() {
        DateTimeMatrixCreator c = new DateTimeMatrixCreator();
        Assert.assertEquals("6",c.convertWeekday("2", 3, false));
        Assert.assertEquals("7",c.convertWeekday("3", 3, false));
        Assert.assertEquals("1",c.convertWeekday("4", 3, false));
        Assert.assertEquals("2",c.convertWeekday("5", 3, false));
        Assert.assertEquals("3",c.convertWeekday("6", 3, false));
        Assert.assertEquals("4",c.convertWeekday("7", 3, false));
        Assert.assertEquals("5",c.convertWeekday("1", 3, false));
    }

    @Test
    public void test2() {
        DateTimeMatrixCreator c = new DateTimeMatrixCreator();
        Assert.assertEquals("4",c.convertWeekday("2", -2, true));
        Assert.assertEquals("5",c.convertWeekday("3", -2, true));
        Assert.assertEquals("6",c.convertWeekday("4", -2, true));
        Assert.assertEquals("0",c.convertWeekday("5", -2, true));
        Assert.assertEquals("1",c.convertWeekday("6", -2, true));
        Assert.assertEquals("2",c.convertWeekday("7", -2, true));
        Assert.assertEquals("3",c.convertWeekday("1", -2, true));
        
    }

    
}
