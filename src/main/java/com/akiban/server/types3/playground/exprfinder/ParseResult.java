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

import java.util.ArrayList;
import java.util.List;

public class ParseResult {
    public List<Declaration> declarations() {
        return declarations;
    }

    public String createQuery() {
        StringBuilder sb = new StringBuilder();
        for (Object element : parseElements) {
            if (element instanceof Declaration) {
                Declaration d = (Declaration) element;
                sb.append(d.columnName());
            }
            else if (element instanceof String) {
                sb.append(element);
            }
            else {
                throw new AssertionError(element.getClass());
            }
        }
        return sb.toString();
    }

    public String explain() {
        StringBuilder sb = new StringBuilder();
        for (Object element : parseElements) {
            if (element instanceof Declaration) {
                sb.append('<').append(element).append('>');
            }
            else if (element instanceof String) {
                sb.append(element);
            }
            else {
                throw new AssertionError(element.getClass());
            }
            sb.append(' ');
        }
        return sb.toString();
    }

    void addStringElement(String element) {
        if (element.length() > 0)
            parseElements.add(element);
    }

    void addDeclarationElement(Declaration declaration) {
        parseElements.add(declaration);
        declarations.add(declaration);
    }

    private List<Object> parseElements = new ArrayList<Object>();
    private List<Declaration> declarations = new ArrayList<Declaration>();
}
