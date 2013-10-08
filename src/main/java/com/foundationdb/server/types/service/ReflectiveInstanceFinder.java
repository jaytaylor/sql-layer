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

package com.foundationdb.server.types.service;

import com.foundationdb.server.error.AkibanInternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;


public class ReflectiveInstanceFinder implements InstanceFinder
{
    private static final Logger logger = LoggerFactory.getLogger(ReflectiveInstanceFinder.class);

    private final Set<Class<?>> searchClasses;
    
    private static final int SKIP = -1;
    private static final int FIELD = 0;
    private static final int ARRAY = 1;
    private static final int COLLECTION = 2;

    public ReflectiveInstanceFinder()
    throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        this(new ConfigurableClassFinder("typedirs.txt"));
    }

    public ReflectiveInstanceFinder(ClassFinder classFinder)
    throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        // collect all scalar TOverload instances
        searchClasses = classFinder.findClasses();
    }

    @Override
    public <T> Collection<? extends T> find(Class<? extends T> targetClass) {
        return collectInstances(searchClasses, targetClass);
    }

    private static <T> Collection<T> collectInstances(Collection<Class<?>> classes, Class<T> target)
    {
        List<T> ret = new ArrayList<>();
        for (Class<?> cls : classes) {
            if (!Modifier.isPublic(cls.getModifiers()))
                continue;
            else
                doCollecting(ret, cls, target);
        }
        return ret;
    }

    private static <T> void doCollecting(Collection<T> ret, Class<?> cls, Class<T> target) 
    {
        try
        {
            // grab the static INSTANCEs fields
            for (Field field : cls.getFields())
            {
                if (isRegistered(field))
                    switch(validateField(field, target))
                    {
                        case FIELD:
                            putItem(ret, field.get(null), target, field);
                            break;
                        case ARRAY:
                            for (Object item : (Object[])field.get(null)) {
                                if (target.isInstance(item))
                                    putItem(ret, item, target, field);
                            }
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)field.get(null)) {
                                    if (target.isInstance(raw))
                                        putItem(ret, raw, target, field);
                                }
                                break;
                            }
                            catch (ClassCastException e) {/* fall thru */}
                        default:
                               // SKIP (does nothing)
                    }
            }
            
            // grab the static methods that create instances
            for (Method method : cls.getMethods())
            {
                
                if (isRegistered(method))
                    switch(validateMethod(method, target))
                    {
                        case FIELD:
                            putItem(ret, method.invoke(null), target, method);
                            break;
                        case ARRAY:
                            for (Object item : (Object[])method.invoke(null))
                                putItem(ret, item, target, method);
                            break;
                        case COLLECTION:
                            try
                            {
                                for (Object raw : (Collection<?>)method.invoke(null))
                                    putItem(ret, raw, target, method);
                                break;
                            }
                            catch (ClassCastException e) {/* fall thru */}
                        default:
                            // SKIP (does nothing)
                    }
            }
           
        }
        catch (IllegalAccessException e)   
        {
            throw new FunctionsRegistryException(e.getMessage());
        }
        catch (InvocationTargetException ex)
        {
            throw new FunctionsRegistryException(ex.getMessage());
        }
    }

    public static class FunctionsRegistryException extends AkibanInternalException {
        public FunctionsRegistryException(String message) {
            super(message);
        }
    }

    private static <T> int validateMethod(Method method, Class<T> target)
    {
        int modifiers = method.getModifiers();
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers)
                && method.getParameterTypes().length == 0)
            return assignable(method.getReturnType(), target, method.getGenericReturnType());
        return SKIP;
    }
    
    private static <T> int validateField(Field field, Class<T> target)
    { 
        int modifiers = field.getModifiers();
        
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers))
            return assignable(field.getType(), target, field.getGenericType());
         return SKIP;
    }

    private static <T> int assignable (Class<?> c, Class<T> target, Type genericTarget)
    {
        if (c.isArray() && target.isAssignableFrom(c.getComponentType())) {
            return ARRAY;
        }
        else if (target.isAssignableFrom(c)) {
            return FIELD;
        }
        else if (Collection.class.isAssignableFrom(c)) {
            if (genericTarget instanceof ParameterizedType) {
                ParameterizedType targetParams = (ParameterizedType) genericTarget;
                Type[] genericArgs = targetParams.getActualTypeArguments();
                assert genericArgs.length == 1 : Arrays.toString(genericArgs);
                Type genericArg = genericArgs[0];
                if (genericArg instanceof WildcardType) {
                    Type[] upperBounds = ((WildcardType)genericArg).getUpperBounds();
                    if (upperBounds.length > 1)
                        logger.debug("multiple upper bounds for {}: {}", genericTarget, Arrays.toString(upperBounds));
                    for (Type upperBound : upperBounds) {
                        if (isAssignableFrom(target, upperBound))
                            return COLLECTION;
                    }
                }
                else if (isAssignableFrom(target, genericArg))
                    return COLLECTION;
            }
        }
        return SKIP;
    }

    private static boolean isAssignableFrom(Class<?> target, Type actualType) {
        return (actualType instanceof Class<?>) && target.isAssignableFrom((Class<?>) actualType);
    }

    private static <T> void putItem(Collection<T> list, Object item,  Class<T> targetClass, Object source)
    {
        T cast;
        try {
            cast = targetClass.cast(item);
        } catch (ClassCastException e) {
            String err = "while casting " + item + " from " + source + " to " + targetClass;
            logger.error(err, e);
            throw new ClassCastException(err);
        }
        list.add(cast);
    }

    private static boolean isRegistered(AccessibleObject field)
    {
        return field.getAnnotation(DontRegister.class) == null;
    }
}
