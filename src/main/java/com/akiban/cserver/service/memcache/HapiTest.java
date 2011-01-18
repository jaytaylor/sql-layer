package com.akiban.cserver.service.memcache;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;

public final class HapiTest {
    public static String[] INPUTS = {
            "coi:customer:name=snowman",
            "coi:customer:name='_%28+%E2%98%83+%29_'",
            "coi:customer:(order)date>NOW"
    };

    public static void main(String[] ignored) throws Exception {
        for(String input : INPUTS) {
            parse(input);
        }
    }

    private static void parse(String string) throws Exception {
        hapiLexer lexer = new hapiLexer( new ANTLRStringStream(string) );
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        hapiParser parser = new hapiParser(tokens);
        HapiGetRequest rv = parser.get_request();
        System.out.println(string);
        System.out.println(rv);
        System.out.println("-------------------------");
    }

    private static void printCommonTree(Object o) {
        System.out.println(o);
    }
}
