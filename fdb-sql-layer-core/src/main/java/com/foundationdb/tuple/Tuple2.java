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

package com.foundationdb.tuple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.foundationdb.Range;

/**
 * Represents a set of elements that make up a sortable, typed key. This object
 *  is comparable with other {@code Tuple}s and will sort in Java in
 *  the same order in which they would sort in FoundationDB. {@code Tuple}s sort
 *  first by the first element, then by the second, etc. This makes the tuple layer
 *  ideal for building a variety of higher-level data models.<br>
 * <h3>Types</h3>
 * A {@code Tuple} can
 *  contain byte arrays ({@code byte[]}), {@link String}s, {@link Number}s, and {@code null}. 
 *  Note that for numbers outside this range the way that Java
 *  truncates integral values may yield unexpected results.<br>
 * <h3>{@code null} values</h3>
 * The FoundationDB tuple specification has a special type-code for {@code None}; {@code nil}; or,
 *  as Java would understand it, {@code null}.
 *  The behavior of the layer in the presence of {@code null} varies by type with the intention
 *  of matching expected behavior in Java. {@code byte[]} and {@link String}s can be {@code null},
 *  where integral numbers (i.e. {@code long}s) cannot.
 *  This means that the typed getters ({@link #getBytes(int) getBytes()} and {@link #getString(int) getString()})
 *  will return {@code null} if the entry at that location was {@code null} and the typed adds
 *  ({@link #add(byte[])} and {@link #add(String)}) will accept {@code null}. The
 *  {@link #getLong(int) typed get for integers}, however, will throw a {@code NullPointerException} if
 *  the entry in the {@code Tuple} was {@code null} at that position.<br>
 * <br>
 * This class is not thread safe.
 */
public class Tuple2 extends Tuple {
    private List<Object> elements;

    private Tuple2(List<? extends Object> elements, Object newItem) {
        this(new LinkedList<Object>(elements));
        this.elements.add(newItem);
    }

    private Tuple2(List<? extends Object> elements) {
        this.elements = new ArrayList<Object>(elements);
    }

    /**
     * Creates a copy of this {@code Tuple} with an appended last element. The parameter
     *  is untyped but only {@link String}, {@code byte[]}, {@link Number}s, and {@code null} are allowed.
     *  All {@code Number}s are converted to a 8 byte integral value, so all floating point
     *  information is lost.
     *
     * @param o the object to append. Must be {@link String}, {@code byte[]},
     *  {@link Number}s, or {@code null}.
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 addObject(Object o) {
        if(o != null &&
                !(o instanceof Boolean) &&
                !(o instanceof String) &&
                !(o instanceof byte[]) &&
                !(o instanceof Number) &&
                !(o instanceof UUID)) {
            throw new IllegalArgumentException("Parameter type (" + o.getClass().getName() + ") not recognized");
        }
        return new Tuple2(this.elements, o);
    }

    /**
     * Creates a copy of this {@code Tuple} with a {@code String} appended as the last element.
     *
     * @param s the {@code String} to append
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 add(String s) {
        return new Tuple2(this.elements, s);
    }

    /**
     * Creates a copy of this {@code Tuple} with a {@code long} appended as the last element.
     *
     * @param l the number to append
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 add(long l) {
        return new Tuple2(this.elements, l);
    }

    /**
     * Creates a copy of this {@code Tuple} with a {@code byte} array appended as the last element.
     *
     * @param b the {@code byte}s to append
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 add(byte[] b) {
        return new Tuple2(this.elements, b);
    }

    public Tuple2 add(Double b) {
        return new Tuple2(this.elements, b);
    }

    public Tuple2 add(UUID u) {
        return new Tuple2(this.elements, u);
    }
       
    public Tuple2 add(Float b) {
        return new Tuple2(this.elements, b);
    }

    public Tuple2 add(BigDecimal b) {
        return new Tuple2(this.elements, b);
    }
    
    public Tuple2 add(BigInteger b) {
        return new Tuple2(this.elements, b);
    }

    public Tuple2 add(Boolean b) {
        return new Tuple2(this.elements, b);
    }

    /**
     * Creates a copy of this {@code Tuple} with a {@code byte} array appended as the last element.
     *
     * @param b the {@code byte}s to append
     * @param offset the starting index of {@code b} to add
     * @param length the number of elements of {@code b} to copy into this {@code Tuple}
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 add(byte[] b, int offset, int length) {
        return new Tuple2(this.elements, Arrays.copyOfRange(b, offset, offset + length));
    }

    /**
     * Create a copy of this {@code Tuple} with a list of items appended.
     *
     * @param o the list of objects to append. Elements must be {@link String}, {@code byte[]},
     *  {@link Number}s, or {@code null}.
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 addAll(List<? extends Object> o) {
        List<Object> merged = new ArrayList<Object>(o.size() + this.elements.size());
        merged.addAll(this.elements);
        merged.addAll(o);
        return new Tuple2(merged);
    }

    /**
     * Create a copy of this {@code Tuple} with all elements from anther {@code Tuple} appended.
     *
     * @param other the {@code Tuple} whose elements should be appended
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 addAll(Tuple2 other) {
        List<Object> merged = new ArrayList<Object>(this.size() + other.size());
        merged.addAll(this.elements);
        merged.addAll(other.peekItems());
        return new Tuple2(merged);
    }

    /**
     * Get an encoded representation of this {@code Tuple}. Each element is encoded to
     *  {@code byte}s and concatenated.
     *
     * @return a serialized representation of this {@code Tuple}.
     */
    @Override
    public byte[] pack() {
        return TupleFloatingUtil.pack(this.elements);
    }

    /**
     * Gets the unserialized contents of this {@code Tuple}.
     *
     * @return the elements that make up this {@code Tuple}.
     */
    public List<Object> getItems() {
        return new ArrayList<Object>(elements);
    }

    /**
     * Returns the internal elements that make up this tuple. For internal use only, as
     *  modifications to the result will mean that this Tuple is modified.
     *
     * @return the elements of this Tuple, without copying
     */
    private List<Object> peekItems() {
        return this.elements;
    }

    /**
     * Gets an {@code Iterator} over the {@code Objects} in this {@code Tuple}. This {@code Iterator} is
     *  unmodifiable and will throw an exception if {@link Iterator#remove() remove()} is called.
     *
     * @return an unmodifiable {@code Iterator} over the elements in the {@code Tuple}.
     */
    @Override
    public Iterator<Object> iterator() {
        return Collections.unmodifiableList(this.elements).iterator();
    }

    /**
     * Construct a new empty {@code Tuple}. After creation, items can be added
     *  with calls the the variations of {@code add()}.
     *
     * @see #from(Object...)
     * @see #fromBytes(byte[])
     * @see #fromItems(Iterable)
     */
    public Tuple2() {
        this.elements = new LinkedList<Object>();
    }

    /**
     * Construct a new {@code Tuple} with elements decoded from a supplied {@code byte} array.
     *
     * @param bytes encoded {@code Tuple} source. Must not be {@code null}
     *
     * @return a newly constructed object.
     */
    public static Tuple2 fromBytes(byte[] bytes) {
        return fromBytes(bytes, 0, bytes.length);
    }

    /**
     * Construct a new {@code Tuple} with elements decoded from a supplied {@code byte} array.
     *
     * @param bytes encoded {@code Tuple} source. Must not be {@code null}
     *
     * @return a newly constructed object.
     */
    public static Tuple2 fromBytes(byte[] bytes, int offset, int length) {
        Tuple2 t = new Tuple2();
        t.elements = TupleFloatingUtil.unpack(bytes, offset, length);
        return t;
    }

    /**
     * Gets the number of elements in this {@code Tuple}.
     *
     * @return the count of elements
     */
    public int size() {
        return this.elements.size();
    }

    /**
     * Determine if this {@code Tuple} contains no elements.
     *
     * @return {@code true} if this {@code Tuple} contains no elements, {@code false} otherwise
     */
    public boolean isEmpty() {
        return this.elements.isEmpty();
    }

    /**
     * Gets an indexed item as a {@code long}. This function will not do type conversion
     *  and so will throw a {@code ClassCastException} if the element is not a number type.
     *  The element at the index may not be {@code null}.
     *
     * @param index the location of the item to return
     *
     * @return the item at {@code index} as a {@code long}
     */
    public long getLong(int index) {
        Object o = this.elements.get(index);
        if(o == null)
            throw new NullPointerException("Number types in Tuples may not be null");
        return (Long)o;
    }

    /**
     * Gets an indexed item as a {@code byte[]}. This function will not do type conversion
     *  and so will throw a {@code ClassCastException} if the tuple element is not a
     *  {@code byte} array.
     *
     * @param index the location of the element to return
     *
     * @return the item at {@code index} as a {@code byte[]}
     */
    public byte[] getBytes(int index) {
        Object o = this.elements.get(index);
        // Check needed, since the null may be of type "Object" and may not be casted to byte[]
        if(o == null)
            return null;
        return (byte[])o;
    }

    /**
     * Gets an indexed item as a {@code String}. This function will not do type conversion
     *  and so will throw a {@code ClassCastException} if the tuple element is not of
     *  {@code String} type.
     *
     * @param index the location of the element to return
     *
     * @return the item at {@code index} as a {@code String}
     */
    public String getString(int index) {
        Object o = this.elements.get(index);
        // Check needed, since the null may be of type "Object" and may not be casted to byte[]
        if(o == null) {
            return null;
        }
        return (String)o;
    }

    /**
     * Gets an indexed item without forcing a type.
     *
     * @param index the index of the item to return
     *
     * @return an item from the list, without forcing type conversion
     */
    public Object get(int index) {
        return this.elements.get(index);
    }

    /**
     * Creates a new {@code Tuple} with the first item of this {@code Tuple} removed.
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 popFront() {
        if(elements.size() == 0)
            throw new IllegalStateException("Tuple contains no elements");


        List<Object> items = new ArrayList<Object>(elements.size() - 1);
        for(int i = 1; i < this.elements.size(); i++) {
            items.add(this.elements.get(i));
        }
        return new Tuple2(items);
    }

    /**
     * Creates a new {@code Tuple} with the last item of this {@code Tuple} removed.
     *
     * @return a newly created {@code Tuple}
     */
    public Tuple2 popBack() {
        if(elements.size() == 0)
            throw new IllegalStateException("Tuple contains no elements");


        List<Object> items = new ArrayList<Object>(elements.size() - 1);
        for(int i = 0; i < this.elements.size() - 1; i++) {
            items.add(this.elements.get(i));
        }
        return new Tuple2(items);
    }

    /**
     * Returns a range representing all keys that encode {@code Tuple}s strictly starting
     *  with this {@code Tuple}.
     * <br>
     * <br>
     * For example:
     * <pre>
     *   Tuple t = Tuple.from("a", "b");
     *   Range r = t.range();</pre>
     * {@code r} includes all tuples ("a", "b", ...)
     *
     * @return the keyspace range containing all {@code Tuple}s that have this {@code Tuple}
     *  as a prefix.
     */
    public Range range() {
        byte[] p = pack();
        //System.out.println("Packed tuple is: " + ByteArrayUtil.printable(p));
        return new Range(ByteArrayUtil.join(p, new byte[] {0x0}),
                         ByteArrayUtil.join(p, new byte[] {(byte)0xff}));
    }

    /**
     * Compare the byte-array representation of this {@code Tuple} against another. This method
     *  will sort {@code Tuple}s in the same order that they would be sorted as keys in
     *  FoundationDB. Returns a negative integer, zero, or a positive integer when this object's
     *  byte-array representation is found to be less than, equal to, or greater than the
     *  specified {@code Tuple}.
     *
     * @param t the {@code Tuple} against which to compare
     *
     * @return a negative integer, zero, or a positive integer when this {@code Tuple} is
     *  less than, equal, or greater than the parameter {@code t}.
     */
    public int compareTo(Tuple2 t) {
        return ByteArrayUtil.compareUnsigned(this.pack(), t.pack());
    }

    /**
     * Returns a hash code value for this {@code Tuple}.
     * {@inheritDoc}
     *
     * @return a hashcode
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(this.pack());
    }

    /**
     * Tests for equality with another {@code Tuple}. If the passed object is not a {@code Tuple}
     *  this returns false. If the object is a {@code Tuple}, this returns true if
     *  {@link Tuple2#compareTo(Tuple2) compareTo()} would return {@code 0}.
     *
     * @return {@code true} if {@code obj} is a {@code Tuple} and their binary representation
     *  is identical.
     */
    @Override
    public boolean equals(Object o) {
        if(o == null)
            return false;
        if(o instanceof Tuple2) {
            return Arrays.equals(this.pack(), ((Tuple2) o).pack());
        }
        return false;
    }

    /**
     * Creates a new {@code Tuple} from a variable number of elements. The elements
     *  must follow the type guidelines from {@link Tuple2#addObject(Object) add}, and so
     *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, or {@code null}s.
     *
     * @param items the elements from which to create the {@code Tuple}.
     *
     * @return a newly created {@code Tuple}
     */
    public static Tuple2 fromItems(Iterable<? extends Object> items) {
        Tuple2 t = new Tuple2();
        for(Object o : items) {
            t = t.addObject(o);
        }
        return t;
    }

    /**
     * Efficiently creates a new {@code Tuple} from a list of objects. The elements
     *  must follow the type guidelines from {@link Tuple2#addObject(Object) add}, and so
     *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, or {@code null}s.
     *
     * @param items the elements from which to create the {@code Tuple}.
     *
     * @return a newly created {@code Tuple}
     */
    public static Tuple2 fromList(List<? extends Object> items) {
        return new Tuple2(items);
    }

    /**
     * Creates a new {@code Tuple} from a variable number of elements. The elements
     *  must follow the type guidelines from {@link Tuple2#addObject(Object) add}, and so
     *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, or {@code null}s.
     *
     * @param items the elements from which to create the {@code Tuple}.
     *
     * @return a newly created {@code Tuple}
     */
    public static Tuple2 from(Object ... items) {
        return fromList(Arrays.asList(items));
    }
}
