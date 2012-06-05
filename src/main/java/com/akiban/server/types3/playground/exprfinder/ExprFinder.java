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

package com.akiban.server.types3.playground.exprfinder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ExprFinder {
    public static void main(String[] args) throws IOException, SQLException {
        Prompt prompt = new Prompt();
        DbAdapter db = dbAdapter(prompt);
        try {
            for (String line : prompt.setPrompt("$ ")) {
                ParseResult parse = parse(line);
                List<Declaration> declarations = parse.declarations();

                db.init();
                db.createTable(declarations);
                String query = parse.createQuery();
                String resultDefinition = db.getResultDefinition(query);

                Map<String,String> inputDefinitions = db.getDefinitions();
                output(parse, inputDefinitions, resultDefinition);
            }
        }
        finally {
            db.close();
        }
    }

    private static void output(ParseResult parse, Map<String,String> inputDefinitions, String resultDefinition) {
        System.out.println(resultDefinition.toUpperCase());
        System.out.println("\tInput: " + parse.explain());
        System.out.println("\tSource columns: " + inputDefinitions);
        System.out.println("\tQuery: " + parse.createQuery());
    }

    private static ParseResult parse(String line) {
        ParseResult result = new ParseResult();
        Matcher matcher = DECLARATION_PATTERN.matcher(line);
        int lastEnd = 0;
        while (matcher.find()) {
            String previous = line.substring(lastEnd, matcher.start());
            lastEnd = matcher.end();

            result.addStringElement(previous);
            Declaration declaration = new Declaration(matcher.group(1), result.declarations().size());
            result.addDeclarationElement(declaration);
        }
        String lastSegment = line.substring(lastEnd);
        result.addStringElement(lastSegment);

        return result;
    }

    private static DbAdapter dbAdapter(Prompt prompt) throws IOException {
        DbAdapter adapter = new MySqlAdapter("t_expr_test", "src", "dst");
        String username = prompt.ask("username: ");
        String password = prompt.ask("password (or blank): ");
        adapter.load(username, password);
        return adapter;
    }

    private static final Pattern DECLARATION_PATTERN = Pattern.compile("<([^>]*)>");
}
