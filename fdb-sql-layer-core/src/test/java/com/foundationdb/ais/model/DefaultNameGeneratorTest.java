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

package com.foundationdb.ais.model;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DefaultNameGeneratorTest
{
    private DefaultNameGenerator generator = new DefaultNameGenerator();

    @Test
    public void identitySequenceName() {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        TableName table = new TableName("test", "t");
        assertEquals(new TableName("test", "t_s1_seq"),
                     generator.generateIdentitySequenceName(ais, table, "s1"));
    }

    @Test
    public void identitySequenceNameConflict() {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Sequence.create(ais, "test", "t_s_seq", 1, 1, 1, 10, false);
        assertEquals(new TableName("test", "t_s_seq$1"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", "t"), "s"));
    }

    @Test
    public void identitySequenceTruncate() {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        char[] chars = new char[DefaultNameGenerator.MAX_IDENT];
        Arrays.fill(chars, 'a');
        String maxIdent = new String(chars);
        // Table too long
        assertEquals(new TableName("test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$1"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", maxIdent), "s"));
        // Serial long
        assertEquals(new TableName("test", "t_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$1"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", "t"), maxIdent));
        // Both long
        assertEquals(new TableName("test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$1"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", maxIdent), maxIdent));

        // Long with conflict
        Sequence.create(ais, "test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$1", 1, 1, 1, 10, false);
        assertEquals(new TableName("test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$2"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", maxIdent), "s"));
    }
}
