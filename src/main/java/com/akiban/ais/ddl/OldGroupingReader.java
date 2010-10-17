package com.akiban.ais.ddl;

import com.akiban.ais.model.staticgrouping.Grouping;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;

public final class OldGroupingReader {

    private GroupDef groupDef;

    public static Grouping fromString(String input) throws Exception {
        return (new OldGroupingReader()).readString(input);
    }

    public Grouping readString(final String input) throws Exception {
        return buildFromString(new DDLSource.StringStream(input));
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
