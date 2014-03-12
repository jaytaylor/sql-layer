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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.AkibanInformationSchema;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PersistitNameGeneratorTest
{
    PersistitNameGenerator generator = new PersistitNameGenerator(new AkibanInformationSchema());

    @Test
    public void checkShortName() {
        // Sanity that short names aren't needless truncated
        assertEquals("s.g", generator.generateGroupTreeName("s", "g"));
        assertEquals("s.s", generator.generateSequenceTreeName("s", "s"));
        assertEquals("s.t.i", generator.generateIndexTreeName("s", "t", "i"));
    }

    @Test
    public void checkLongNames() {
        String longName = createLongString();
        // group
        checkLength(generator.generateGroupTreeName(longName, "g"));
        checkLength(generator.generateGroupTreeName("s", longName));
        checkLength(generator.generateGroupTreeName(longName, longName));
        // sequence
        checkLength(generator.generateSequenceTreeName(longName, "s"));
        checkLength(generator.generateSequenceTreeName("s", longName));
        checkLength(generator.generateSequenceTreeName(longName, longName));
        // index
        checkLength(generator.generateIndexTreeName(longName, "t", "i"));
        checkLength(generator.generateIndexTreeName("s", longName, "i"));
        checkLength(generator.generateIndexTreeName("s", "t", longName));
        checkLength(generator.generateIndexTreeName(longName, longName, "i"));
        checkLength(generator.generateIndexTreeName("s", longName, longName));
        checkLength(generator.generateIndexTreeName(longName, longName, longName));
        assertEquals("total trees", 12, generator.treeNames.size());
    }

    private static void checkLength(String s) {
        assertTrue("generated name length", s.length() <= PersistitNameGenerator.MAX_TREE_NAME_LENGTH);
    }

    private static String createLongString() {
        char[] chars = new char[PersistitNameGenerator.MAX_TREE_NAME_LENGTH * 2];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }
}
