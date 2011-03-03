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

package com.akiban.ais.ddl;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;

import com.akiban.ais.model.staticgrouping.Grouping;

public final class OldGroupingReader {

    private GroupDef groupDef;

    public static Grouping fromString(String input) throws Exception {
        return (new OldGroupingReader()).readString(input);
    }

    public Grouping readString(final String input) throws Exception {
        return buildFromString(new SchemaDef.SDStringStream(input));
    }

    private Grouping buildFromString(final ANTLRStringStream stringStream) throws Exception {
        OldGroupingLexer lex = new OldGroupingLexer(stringStream);
        CommonTokenStream tokens = new CommonTokenStream(lex);
        final OldGroupingParser parser = new OldGroupingParser(tokens);
        groupDef = new GroupDef();
        parser.old_grouping(groupDef);

        if (parser.getNumberOfSyntaxErrors() > 0) {
            throw new RuntimeException("reported a syntax error(s): " + parser.getNumberOfSyntaxErrors());
        }

        return groupDef.getGroupsBuilder().getGrouping();
    }
}
