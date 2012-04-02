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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Simple utility class for reading a SQL log file and returning back its
 * statements one at a time.
 * 
 * This is a very simple "parser", which doesn't actually do any real parsing.
 * It knows about MySQL quotes, comments and semicolons.
 */
public class MySqlStatementSplitter implements Iterable<String> {

    /**
     * A hook that allows for modifying the generated SQL statements as they're created.
     */
    public interface ReadWriteHook {
        /**
         * Called when a non-quoted, non-commented, non-newline char is read in. This method is called <em>after</em>
         * the char has been appended to the builder, so the last char in the builder is the one we just saw.
         * @param builder the StringBuilder that's storing the string that's being built.
         */
        public void seeChar(StringBuilder builder);
    }

    private final Reader stream;
    private final String convertNewlines;
    private final boolean keepSpecialComments;
    private final Builder builder;

    private String pending = null;
    private boolean isDone = false;
    private String remainder = null;

    private long charsRead = 0;
    private int buffer = -1;

    private static final int COMMENT_TO_EOL = 1;
    private static final int COMMENT_C_STYLE = 2;
    private static final int COMMENT_C_SPECIAL_STYLE = 3;

    private final Iterator<String> internalIterator = new Iterator<String>() {
        @Override
        public boolean hasNext() {
            if (pending != null) {
                return true;
            }
            return ((pending = next()) != null);
        }

        @Override
        public String next() {
            return parse();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    };

    private static class Builder {
        private static final int MATCH_NO = -1;
        private static final int MATCH_MAYBE = 0;
        private static final int MATCH_YES = 1;
        private final boolean trimLines;

        private final StringBuilder builder = new StringBuilder();
        private boolean trimmed = false;
        private ReadWriteHook hook = null;

        private int matched = MATCH_MAYBE;
        private final char[] matchTo;
        private int matchIndex = 0;

        private Builder(boolean trimLines, char[] matchTo) {
            this.trimLines = trimLines;
            this.matchTo = matchTo;
        }

        public void setHook(ReadWriteHook hook) {
            this.hook = hook;
        }

        public void applyHook() {
            if (hook != null) {
                hook.seeChar(builder);
            }
        }

        private void reset() {
            builder.setLength(0);
            trimmed = !trimLines;
            matched = (matchTo == null) ? MATCH_YES : MATCH_MAYBE;
            matchIndex = -1;
        }

        private boolean trimmed() {
            return trimmed;
        }

        private boolean matchesPrefix() {
            return matched == MATCH_YES;
        }

        public void newline(String convertNewlines) {
            if (trimmed) {
                builder.append(convertNewlines);
            }
        }

        private void append(int theInt) {
            if (matched == MATCH_NO) {
                return;
            }
            if (!trimmed) {
                trimmed = ((char) theInt) > '\u0020';
            }
            if (trimmed) {
                builder.append((char) theInt);
                if (matchIndex < 0) {
                    matchIndex = ((char) theInt) > '\u0020' ? 0 : -1;
                }
                if ((matchIndex >= 0) && (matched == MATCH_MAYBE)) {
                    if (matchIndex < matchTo.length) {
                        if (matchTo[matchIndex] != Character
                                .toLowerCase((char) theInt)) {
                            matched = MATCH_NO;
                        } else if (++matchIndex == matchTo.length) {
                            matched = MATCH_YES;
                        }
                    } else {
                        matched = MATCH_NO;
                    }
                }
            }
        }

        private void append(String theString) {
            builder.append(theString);
        }

        @Override
        public String toString() {
            return builder.toString();
        }
    }

    /**
     * Creates a parser with defaults.
     * 
     * Equivalent to {@linkplain #MySqlStatementSplitter}(reader, " ", true, true).
     * 
     * @param reader
     */
    public MySqlStatementSplitter(Reader reader) {
        this(reader, " ", true, true, null);
    }

    public void setHook(ReadWriteHook hook) {
        builder.setHook(hook);
    }

    /**
     * Creates a parser with various options.
     * 
     * <p>
     * If convertNewlines is non-null, any newline will be replaced by the given
     * string. Newlines in quotes will not be affected by this replacement. <br/>
     * A newline is one of the following: \n \r \n\r \r\n
     * </p>
     * 
     * <p>
     * The results can optionally be pre-trimmed. This uses the same algorithm
     * that String.trim() uses: whitespace is any character whose int value &gt;
     * '\u0020'. This only matters for the beginning of the string: the end will
     * never have trailing characters.
     * </p>
     * 
     * @param reader
     * @param convertNewlines
     * @param keepSpecialComments
     *            if true, <code>/*! MySQL special comments*<!-- -->/</code>
     *            will be kept
     * @param trimLines
     *            whether to pre-trim the results
     */
    public MySqlStatementSplitter(Reader reader, String convertNewlines,
            boolean keepSpecialComments, boolean trimLines,
            String requiredPrefix) {
        this.stream = new BufferedReader(reader);
        this.convertNewlines = convertNewlines;
        this.keepSpecialComments = keepSpecialComments;

        final char[] matchTo;
        if (requiredPrefix != null) {
            final int len = requiredPrefix.length();
            if (len == 0) {
                matchTo = null;
            } else {
                matchTo = new char[len];
                for (int i = 0; i < len; i++)
                    matchTo[i] = Character
                            .toLowerCase(requiredPrefix.charAt(i));
            }
        } else {
            matchTo = null;
        }
        builder = new Builder(trimLines, matchTo);
    }

    public long getCharsRead() {
        return charsRead;
    }

    /**
     * Gets the next line.
     */
    public String parse() {
        // If we have a String pending, return that
        if (pending != null) {
            String ret = pending;
            pending = null;
            return ret;
        }

        builder.reset();

        int theInt;
        int quoteChar = -1;
        int commentMode = -1;
        boolean nextCharEscaped = false;

        while ((theInt = getChar()) >= 0) {
            ++charsRead;
            if (commentMode == COMMENT_TO_EOL) {
                if (theInt == '\n' || theInt == '\r') {
                    commentMode = -1;
                    buffer(theInt);
                }
                continue;
            }
            if ((commentMode == COMMENT_C_STYLE)
                    || (commentMode == COMMENT_C_SPECIAL_STYLE)) {
                if (commentMode == COMMENT_C_SPECIAL_STYLE) {
                    builder.append(theInt);
                }
                if (theInt == '*') {
                    int nextChar = getChar();
                    if (nextChar == '/') {
                        if (commentMode == COMMENT_C_SPECIAL_STYLE) {
                            builder.append('/');
                        }
                        commentMode = -1;
                    } else {
                        buffer(nextChar);
                    }
                }
                continue;
            }

            boolean charIsEscaped = nextCharEscaped;
            nextCharEscaped = (!nextCharEscaped) && (theInt == '\\');
            if (quoteChar > 0) {
                builder.append(theInt);
                if ((!charIsEscaped) && (theInt == quoteChar)) {
                    quoteChar = -1;
                }
//                charIsEscaped = (!charIsEscaped) && (theInt == '\\');
                continue;
            }

            // Okay, we're not in comment mode or quote mode...
            int nextChar = -1; // if nextChar >= 0 at the end of this, we'll
                               // buffer it
            int nextNextChar = -1;
            switch (theInt) {
            case '#':
                commentMode = COMMENT_TO_EOL;
                break;
            case '/':
                if ((nextChar = getChar()) == '*') {
                    if (keepSpecialComments
                            && (nextNextChar = getChar()) == '!') {
                        commentMode = COMMENT_C_SPECIAL_STYLE;
                        builder.append("/*!");
                    } else {
                        commentMode = COMMENT_C_STYLE;
                    }
                    nextChar = -1;
                    nextNextChar = -1;
                } else {
                    nextNextChar = nextChar;
                    nextChar = theInt;
                }
                break;
            case '-':
                if ((nextChar = getChar()) == '-') {
                    commentMode = COMMENT_TO_EOL;
                    nextChar = -1;
                    // nextNextChar = getChar();
                    // if (nextNextChar == ' ')
                    // {
                    // commentMode = COMMENT_TO_EOL;
                    // nextChar = -1;
                    // nextNextChar = -1;
                    // }
                    // // We treat -- followed by newline as a comment -- just
                    // eat the newline
                    // else if ( (nextNextChar == '\n') || (nextNextChar ==
                    // '\r') )
                    // {
                    // nextChar = handleNewline(nextNextChar);
                    // nextNextChar = -1;
                    // }
                    // // Else, it's not a comment. We append one '-' for
                    // theInt's value.
                    // // nextChar and nextNext char will automatically get
                    // appeneded.
                    // else
                    // {
                    // builder.append('-');
                    // }
                } else {
                    nextNextChar = nextChar;
                    nextChar = theInt;
                }
                break;
            case '\'':
            case '"':
            case '`':
                builder.append(theInt);
                if (!charIsEscaped)
                {
                    quoteChar = theInt;
                }
                break;
            case '\n':
            case '\r':
                if (builder.trimmed()) {
                    // If we have a \n or \r, we'll get it plus its twin \r or
                    // \n (respectively).
                    // handleNewLine will return the next char, OR -1 if a twin
                    // \r or \n was found.
                    // So, if (and only if) we have a next char, we'll buffer
                    // it.
                    int toBuffer = handleNewline(theInt);
                    if (toBuffer >= 0) {
                        buffer(toBuffer);
                    }
                }
                break;
            case ';':
                builder.append(';');
                if (builder.matchesPrefix()) {
                    return builder.toString();
                }
                builder.reset();
                break;
            default:
                builder.append(theInt);
                builder.applyHook();
            }
            if (nextChar == '\n' || nextChar == '\r') {
                buffer(nextChar);
            } else if (nextChar >= 0) {
                if (builder.trimmed() || (((char) nextChar) > '\u0020')) {
                    builder.append(nextChar);
                    builder.applyHook();
                }
                if ((nextNextChar >= 0)
                        && (builder.trimmed() || (((char) nextNextChar) > '\u0020'))) {
                    builder.append(nextNextChar);
                    builder.applyHook();
                }
            }
        }
        remainder = builder.toString();
        return null;
    }

    public String getRemainder() {
        return remainder;
    }

    private int handleNewline(int nl) {
        if (convertNewlines == null) {
            builder.append((char) nl);
        } else {
            builder.newline(convertNewlines);
        }

        char other = (nl == '\n') ? '\r' : '\n';
        int nextChar = getChar();

        if (nextChar == other) {
            if (convertNewlines == null) {
                builder.append(other);
            }
            nextChar = -1;
        }
        return nextChar;
    }

    private int getChar() {
        if (isDone) {
            return -1;
        }
        if (buffer >= 0) {
            int ret = buffer;
            buffer = -1;
            return ret;
        }
        try {
            int ret = stream.read();
            if (ret < 0) {
                isDone = true;
            }
            return ret;
        } catch (IOException e) {
            isDone = true;
            return -1;
        }
    }

    private void buffer(int theChar) {
        assert buffer < 0 : "buffer is full";
        buffer = theChar;
    }

    /**
     * Returns a List of the rest of the lines.
     * 
     * This will not pick up from the beginning; if
     * you have already read any lines, they will not be in the returned List.
     * 
     * Similarly, after you call this method, ready() will always return false,
     * and next() will always return null.
     * 
     * @return a List of Strings that haven't been read before
     */
    public List<String> asList() {
        LinkedList<String> ret = new LinkedList<String>();
        for (String line : this) {
            ret.add(line);
        }
        return ret;
    }

    @Override
    public Iterator<String> iterator() {
        return internalIterator;
    }
}
