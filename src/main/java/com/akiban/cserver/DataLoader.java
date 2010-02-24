package com.akiban.cserver;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Primitive loader for SQL data.
 * 
 * @author peter
 * 
 */
public class DataLoader {

	private InputStream stream;

	private State state;

	private List<Object> valueList = new ArrayList<Object>();

	private Object[] values;
	
	private String tableName;

	private int putBack = -1;
	
	private int rows = 0;

	private enum State {
		INSERT, INTO, TABLENAME, VALUES, VALUE_LIST, VALUE_NEXT, VALUE, END,
	}

	public final static void main(final String[] args) throws Exception {
		for (int index = 0; index < args.length; index++) {
			final String pathName = args[index];
			if (pathName.endsWith(".zip")) {
				final ZipFile zfile = new ZipFile(pathName);
				for (final Enumeration<? extends ZipEntry> entries = zfile
						.entries(); entries.hasMoreElements();) {
					final ZipEntry entry = entries.nextElement();
					final InputStream stream = zfile.getInputStream(entry);
					final DataLoader loader = new DataLoader(stream);
					loader.reset();
					while (loader.parseNextRow());
					System.out.println("loaded " + loader.rows + " rows");
				}
			}
		}
	}

	public DataLoader(final InputStream stream) {
		this.stream = stream;
	}

	public void reset() throws IOException {
		rows = 0;
		state = State.INSERT;
		tableName = null;
		valueList.clear();
		putBack = -1;
	}
	
	
	public boolean parseNextRow() throws IOException {
		int rowCount = rows;
		while (state != State.END && rows == rowCount) {
			token();
		}
		return rows == rowCount + 1;
	}
	
	public Object[] getValues() {
		return values;
	}
	
	public String getTableName() {
		return tableName;
	}
	

	private void token() throws IOException {
		switch (state) {
		case INSERT: {
			String w = word();
			if ("INSERT".equalsIgnoreCase(w)) {
				state = State.INTO;
			}
			break;
		}

		case INTO: {
			String w = word();
			if ("INTO".equalsIgnoreCase(w)) {
				state = State.TABLENAME;
			} else if (state != State.END) {
				state = State.INSERT;
			}
			break;
		}
		case TABLENAME: {
			tableName = word();
			state = State.VALUES;
			break;
		}
		case VALUES: {
			String w = word();
			if ("VALUES".equalsIgnoreCase(w)) {
				state = State.VALUE_LIST;
			} else if (state != State.END) {
				state = State.INSERT;
			}
		}

		case VALUE_LIST: {
			char c = (char) symbol();
			if (c == '(') {
				state = State.VALUE;
			} else if (c == ';') {
				tableName = null;
				state = State.INSERT;
			} else if (c != ',') {
				error("Expected '(' or ';' but got '" + c + "'");
				state = State.INSERT;
			}
			break;
		}
		case VALUE: {
			Object value = atom();
			valueList.add(value);
			state = State.VALUE_NEXT;
			break;
		}

		case VALUE_NEXT: {
			char c = (char) symbol();
			if (c == ')') {
				emitRow();
				state = State.VALUE_LIST;
			} else if (c == ',') {
				state = State.VALUE;
			} else {
				error("Expected ',' or ')' but got '" + c + "'");
			}
			break;
		}
		case END:
			break;
		}
	}

	private void error(final String msg) {
		System.err.println(msg);
	}

	private void emitRow() {
		//System.out.println("INSERT INTO " + tableName + " (" + valueList + ")");
		if (values == null || values.length != valueList.size()) {
			values = new Object[valueList.size()];
		}
		values = valueList.toArray(values);
		valueList.clear();
		rows++;
	}

	// Note: the quote symbol is a backtick
	private String word() throws IOException {
		StringBuilder sb = new StringBuilder();
		char c;
		boolean quoted = false;
		boolean first = true;
		while (true) {
			int v = nextChar();
			if (v == -1) {
				break;
			}
			c = (char) v;
			if (first && c == '`') {
				quoted = true;
				first = false;
				continue;
			}
			if (quoted && c == '`') {
				break;
			}
			if (!quoted && Character.isWhitespace(c)) {
				if (sb.length() == 0) {
					continue;
				} else {
					break;
				}
			}
			if (first && !Character.isLetterOrDigit(c)) {
				sb.append(c);
				break;
			}
			sb.append(c);
			first = false;
		}
		return sb.toString();

	}

	private int symbol() throws IOException {
		while (true) {
			int v = nextChar();
			if (v == -1) {
				return v;
			}
			if (!Character.isWhitespace((char) v)) {
				return v;
			}
		}
	}

	// Note: the quote symbol is a single-quote
	private Object atom() throws IOException {
		StringBuilder sb = new StringBuilder();
		char c;
		boolean quoted = false;
		boolean quotedChar = false;
		boolean first = true;
		while (true) {
			int v = nextChar();
			if (v == -1) {
				break;
			}
			c = (char) v;
			if (c == '\\') {
				quotedChar = true;
				continue;
			}
			if (quotedChar) {
				quotedChar = false;
				switch (c) {
				case 'n':
					sb.append('\n');
					break;
				case 't':
					sb.append('\t');
					break;
				case 'r':
					sb.append('\r');
				default:
					sb.append(c);
					break;
				}
				first = false;
				continue;
			}
			if (first && c == '\'') {
				quoted = true;
				first = false;
				continue;
			}
			if (quoted && c == '\'') {
				break;
			}
			if (!quoted && Character.isWhitespace(c)) {
				if (sb.length() == 0) {
					continue;
				} else {
					break;
				}
			}
			if (!quoted && (c == ')' || c == ',')) {
				putBack = c;
				break;
			}
			if (first && !Character.isLetterOrDigit(c) && c != '-') {
				sb.append(c);
				break;
			}
			sb.append(c);
			first = false;
		}
		if (quoted) {
			return sb.toString();
		} else if (sb.length() == 0) {
			return "";
		} else if (sb.length() == 4 && sb.toString().equalsIgnoreCase("NULL")) {
			return null;
		} else if (isNumber(sb)) {
			return Long.valueOf(Long.parseLong(sb.toString()));
		} else {
			return sb.toString();
		}
	}

	private boolean isNumber(final StringBuilder sb) {
		for (int index = 0; index < sb.length(); index++) {
			final char c = sb.charAt(index);
			if (!Character.isDigit(c) && !(index == 0 && c == '-')) {
				return false;
			}
		}
		return sb.length() > 0;
	}

	private int nextChar() throws IOException {
		if (putBack != -1) {
			int v = putBack;
			putBack = -1;
			return v;
		} else {
			int v = stream.read();
			if (v == -1) {
				state = State.END;
			}
			return v;
		}
	}
}
