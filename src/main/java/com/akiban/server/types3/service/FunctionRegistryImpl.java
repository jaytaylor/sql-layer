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

package com.akiban.server.types3.service;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.service.functions.FunctionsRegistryImpl.FunctionsRegistryException;
import com.akiban.server.types3.TOverload;
import com.google.inject.Singleton;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Singleton
public class FunctionRegistryImpl implements FunctionRegistry
{
    // TODO : define aggregates here
    private List<TOverload> scalars;

    private static final int INVALID = -1;
    private static final int FIELD = 0;
    private static final int ARRAY = 1;
    private static final int COLLECTION = 2;

    FunctionRegistryImpl (FunctionsClassFinder finder) 
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        scalars = new ArrayList<TOverload>();
        for (Class<?> cls : finder.findClasses())
        {
            if (!Modifier.isPublic(cls.getModifiers()))
                continue;
            collectScalars(scalars, cls);
        }
    }

    private static void collectScalars(List<TOverload> list, Class<?> cls)
    {   
        try
        {
            // get the static INSTANCEs
            for (Field field : cls.getDeclaredFields())
            {
                Scalar annotation = field.getAnnotation(Scalar.class);
                if (annotation != null)
                    switch (validateScalarField(field))
                    {
                        case FIELD:
                            putOverload((TOverload) field.get(null), list);
                            break;
                        case ARRAY:
                            for (TOverload overload : (TOverload[]) field.get(null))
                                putOverload(overload, list);
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)field.get(null)) 
                                    putOverload((TOverload)raw, list);
                                break;
                            }
                            catch (ClassCastException e){}
                            // fall thru
                        default:
                            complain("Field " + field 
                                    + " must be declared as public static final TOverload "
                                    + " or public static final TOverload[]"
                                    + " or public static final Collection<? extends TOverload>");
                    }
            }

            // get the static methods
            for (Method method : cls.getDeclaredMethods())
            {
                Scalar annotation = method.getAnnotation(Scalar.class);
                if (annotation != null)
                    switch(validateScalarMethod(method))
                    {
                        case FIELD:
                            putOverload((TOverload)method.invoke(null), list);
                            break;
                        case ARRAY:
                            for (TOverload overload : (TOverload[])method.invoke(null))
                                putOverload(overload, list);
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)method.invoke(null))
                                    putOverload((TOverload)raw,  list);
                                break;
                            }
                            catch (ClassCastException e) {}
                            // fall thru
                        default:
                            complain("Method " + method 
                                    + " must be declared as public static TOverload[] <methodname>() "
                                    + " or public satic Collection<TOverload> <method name>() "
                                    + " or public static TOverload <method name>()");
                    }
            }
        }
        catch (IllegalAccessException e)   
        {
            throw new AkibanInternalException(e.getMessage());
        }
        catch (InvocationTargetException ex)
        {
            throw new AkibanInternalException(ex.getMessage());
        }
    }

    private static int validateScalarMethod(Method method)
    {
        int modifiers = method.getModifiers();
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers)
                && method.getParameterTypes().length == 0)
            return assignable(method.getReturnType());
        return INVALID;
    }
    
    private static void putOverload(TOverload overload, Collection<TOverload> list)
    {
        list.add(overload);
    }

    private static String normalise(String name)
    {
        return name.toLowerCase();
    }

    private static int validateScalarField(Field field)
    { 
        int modifiers = field.getModifiers();
        
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers))
            return assignable(field.getType());
         return INVALID;
    }

    private static int assignable (Class<?> c)
    {
        if (c.isArray() && TOverload.class.isAssignableFrom(c.getComponentType()))
            return ARRAY;
        else if (TOverload.class.isAssignableFrom(c))
            return FIELD;
        else return COLLECTION;
    }

    private static void  complain (String st)
    {
        throw new FunctionsRegistryException(st);
    }

    @Override
    public FunctionKind getFunctionKind(String name)
    {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public Collection<TOverload> overloads()
    {
        return scalars;
    }
}
