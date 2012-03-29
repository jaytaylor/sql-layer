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

package com.akiban.util;

/**
 * <p>
 * A simple command line argument parser the provides primitive type conversion
 * and value checking. An application passes a template and the command line
 * argument array to the constructor of this class. The constructor validates
 * the argument list. Subsequently the application can access specific fields
 * from the command line using the <tt>ArgParser</tt>'s accessor methods.
 * </p>
 * <p>
 * If the command line includes the argument <tt>-?</tt> then <tt>ArgParser</tt>
 * displays a summary of permissible arguments and sets its UsageOnly property
 * to true. The calling application should simply exit if <tt>isUsageonly</tt>
 * is true for the constructed ArgParser. The display text output is in English.
 * Applications needing localization should use a different mechanism.
 * </p>
 * 
 * @author pbeaman
 * @version 1.0
 */
public class ArgParser {
    private String _progName;
    private String[] _template;
    private String _flags;
    private String[] _strArgs;
    private int[] _intArgs;
    private boolean[] _specified;
    private boolean _usageOnly;

    /**
     * <p>
     * Construct an instance that parses an array of command line arguments
     * according to specifications provided in a template. The supplied template
     * is an array of specification elements.
     * </p>
     * <p>
     * Command line arguments are specified by name, not by position. Each
     * argument must either be a flag in the form <tt>-<i>X</i></tt> (where
     * <i>X</i> is letter) or a name/value pair in the form
     * <tt><i>argname</i>=</i>value</i></tt>. The permissible flags and argument
     * names are specified by the array of template strings, each of which must
     * have the form:
     * </p>
     * </p> <blockquote>
     * 
     * <pre>
     * <tt>  _flags|<i>flchar</i>|<i>description</i></tt>
     *         or
     * <tt>  <i>argname</i>|<i>argtype</i>|<i>description</i></tt>
     * </pre>
     * 
     * </blockquote> </p>
     * <p>
     * where
     * </p>
     * <dl>
     * <dt><tt><i>flchar</i></tt></dt>
     * <dd>is a single letter that can be used as a flag on the command line.
     * For example, to allow a the flag "-x", use a template string of the form
     * <tt>_flags|x|Enable the x option</tt>. <</dd>
     * <dt><tt><i>argname</i></tt></dt>
     * <dd>Parameter name.</dd>
     * </dd>
     * <dt><tt><i>argtype</i></tt></dt>
     * <dd>
     * 
     * <pre>
     * <tt>int:<i>defaultvalue</i>:<i>lowbound</i>:<i>highbound</i></tt>
     *        or
     * <tt>String:<i>default</i></tt>
     * </pre>
     * 
     * </dd>
     * </dl>
     * </p>
     */
    public ArgParser(String progName, String[] args, String[] template) {
        _progName = progName;
        _template = template;
        _flags = "";
        _strArgs = new String[template.length];
        _intArgs = new int[template.length];
        _specified = new boolean[template.length];

        String flagsTemplate = "";
        for (int i = 0; i < template.length; i++) {
            if (template[i].startsWith("_flag|")) {
                flagsTemplate += piece(template[i], '|', 1);
            } else {
                doField(null, i);
            }
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                for (int j = 1; j < arg.length(); j++) {
                    int ch = arg.charAt(j);
                    if (ch == '?')
                        usage();
                    else if (flagsTemplate.indexOf(ch) >= 0)
                        _flags += (char) ch;
                    else
                        throw new IllegalArgumentException("Invalid flag ("
                                + (char) ch + ") in " + arg);
                }
            } else {
                String fieldName = piece(args[i], '=', 0);
                int position = lookupName(fieldName);
                if (position < 0)
                    throw new IllegalArgumentException(
                            "No such parameter name " + fieldName
                                    + " in argument " + arg);
                String argValue = arg.substring(fieldName.length() + 1);
                doField(argValue, position);
            }
        }
    }

    /**
     * Display a discription of the permissible argument values to
     * {@link java.lang.System#out}.
     */
    public void usage() {
        _usageOnly = true;
        StringBuffer sb = new StringBuffer();
        System.out.println();
        System.out.println("Usage: java " + _progName + " arguments");
        for (int i = 0; i < _template.length; i++) {
            sb.setLength(0);
            String t = _template[i];
            if (t.startsWith("_flag|")) {
                sb.append("  flag -");
                sb.append(piece(t, '|', 1));
                tab(sb, 24);
                sb.append(piece(t, '|', 2));
                System.out.println(sb);
            } else {
                sb.append("  ");
                sb.append(piece(t, '|', 0));
                tab(sb, 24);
                sb.append(piece(t, '|', 2));
                System.out.println(sb);
            }
        }
        System.out.println();
    }

    /**
     * @return <i>true</i> if the command line arguments contain a help flag,
     *         <tt>-?</tt>.
     */
    public boolean isUsageOnly() {
        return _usageOnly;
    }

    /**
     * @param ch
     *            The flag character
     * @return <i>true</i> if the command line arguments contain the specified
     *         flag.
     */
    public boolean isFlag(int ch) {
        return _flags.indexOf(ch) >= 0;
    }

    /**
     * @return A String containing all the flag characters specified in the
     *         command line arguments. For example, if the command line
     *         specifies value flags <tt>-a -b -c</tt> then this method returns
     *         <tt>"abc"</tt>.
     */
    public String getFlags() {
        return _flags;
    }

    /**
     * @param position
     *            Index to a String value specification in the template array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    private String getStringValue(int position) {
        return _strArgs[position];
    }

    /**
     * @param fieldName
     *            Argname of a String value specification in the template array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    public String getStringValue(String fieldName) {
        return _strArgs[lookupName(fieldName)];
    }

    /**
     * @param position
     *            Index to an int value specification in the template array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    private int getIntValue(int position) {
        return _intArgs[position];
    }

    /**
     * @param fieldName
     *            Argname of an int value specification in the template array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    public int getIntValue(String fieldName) {
        return getIntValue(lookupName(fieldName));
    }

    private int lookupName(String name) {
        String fieldName1 = name + '|';
        for (int i = 0; i < _template.length; i++) {
            if (_template[i].startsWith(fieldName1))
                return i;
        }
        return -1;
    }

    private void doField(String arg, int position) {
        String type = piece(_template[position], '|', 1);
        String t = piece(type, ':', 0);
        if (arg == null)
            arg = piece(type, ':', 1);
        if ("int".equals(t)) {
            int lo = intVal(piece(type, ':', 2));
            int hi = intVal(piece(type, ':', 3));
            int argInt = intVal(arg);
            if (argInt == Integer.MIN_VALUE || argInt < lo || argInt > hi) {
                throw new IllegalArgumentException("Invalid argument "
                        + piece(_template[position], '|', 0) + "=" + arg);
            }
            _intArgs[position] = argInt;
        } else
            _strArgs[position] = arg;
    }

    private int intVal(String s) {
        if (s.length() == 0)
            return 0;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
        }
        return Integer.MIN_VALUE;
    }

    private void tab(StringBuffer sb, int count) {
        while (sb.length() < count) {
            sb.append(' ');
        }
    }

    private String piece(String str, char delimiter, int count) {
        int p = -1;
        int q = -1;
        for (int i = 0; i <= count; i++) {
            if (p == str.length())
                return "";
            q = p;
            p = str.indexOf(delimiter, p + 1);
            if (p == -1)
                p = str.length();
        }
        return str.substring(q + 1, p);
    }
}
