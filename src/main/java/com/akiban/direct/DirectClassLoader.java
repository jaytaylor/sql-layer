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
package com.akiban.direct;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;

import com.akiban.ais.model.AkibanInformationSchema;

/**
 * ClassLoader that generates Akiban Direct classes. There is one instance of
 * this ClassLoader per schema per AkibanInformationSchema instance. The method
 * {@link AkibanInformationSchema#getDirectClassLoader(String, ClassLoader)}
 * should be used to acquire or create an instance of this class.
 * 
 * @author peter
 * 
 */
public class DirectClassLoader extends ClassLoader {

    final AkibanInformationSchema ais;
    final String schemaName;

    final ClassPool pool;
    final Set<String> generated = new HashSet<String>();

    Class<?> extentClass;
    boolean isGenerated;

    public DirectClassLoader(final ClassLoader parentLoader, final String schemaName, final AkibanInformationSchema ais) {
        super(parentLoader);
        this.pool = new ClassPool(true);
        this.schemaName = schemaName;
        this.ais = ais;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return loadClass(name, false);
    }

    /**
     * <p>
     * Implementation of {@link ClassLoader#loadClass(String, boolean)} to load
     * classes generated from the schema. These have been precompiled by
     * Javassist. As required to satisfy links in application code within the
     * module these are reduced to byte code and defined here.
     * </p>
     */
    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> cl = null;
        if (name.startsWith(ClassBuilder.PACKAGE)) {
            ensureGenerated();
        }
        if (cl == null) {
            cl = getParent().loadClass(name);
        }
        if (resolve) {
            resolveClass(cl);
        }
        return cl;
    }

    synchronized void ensureGenerated() {
        if (!isGenerated) {
            try {
                /*
                 * Lazily generate Direct classes from schema
                 */
                if (ais != null && schemaName != null && schemaName.length() > 0) {
                    Map<Integer, CtClass> generatedClasses = ClassBuilder.compileGeneratedInterfacesAndClasses(ais,
                            schemaName);
                    this.registerDirectObjectClasses(generatedClasses);
                }
                isGenerated = true;
            } catch (Exception e) {
                throw new DirectException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void registerDirectObjectClasses(Map<Integer, CtClass> implClasses) throws IOException,
            CannotCompileException {
        try { // TODO
            for (final Entry<Integer, CtClass> entry : implClasses.entrySet()) {
                CtClass c = entry.getValue();
                generated.add(c.getName());
                byte[] b = c.toBytecode();
                Class<? extends DirectObject> cl = (Class<? extends DirectObject>) defineClass(c.getName(), b, 0,
                        b.length);
                resolveClass(cl);
                if (!cl.isInterface()) {
                    Class<?> iface = cl.getInterfaces()[0];
                    Direct.registerDirectObjectClass(iface, (Class<? extends AbstractDirectObject>) cl);
                }
                if (entry.getKey() == Integer.MAX_VALUE) {
                    extentClass = cl;
                }
            }
        } catch (IOException | CannotCompileException e) {
            throw e;
        }
    }

    public void close() throws IOException {
        Direct.unregisterDirectObjectClasses();
    }

    public Class<?> getExtentClass() {
        return extentClass;
    }

}
