/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.bytecode;

import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

/**
 * 包装器
 * 用于创建某个对象方法调用的包装器，避免反射
 * 比如创建SunshineService的Wrapper
 */
public abstract class Wrapper {
    private static final Map<Class<?>, Wrapper> WRAPPER_MAP = new ConcurrentHashMap<Class<?>, Wrapper>(); //class wrapper map
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final String[] OBJECT_METHODS = new String[]{"getClass", "hashCode", "toString", "equals"};
    private static final Wrapper OBJECT_WRAPPER = new Wrapper() {
        @Override
        public String[] getMethodNames() {
            return OBJECT_METHODS;
        }

        @Override
        public String[] getDeclaredMethodNames() {
            return OBJECT_METHODS;
        }

        @Override
        public String[] getPropertyNames() {
            return EMPTY_STRING_ARRAY;
        }

        @Override
        public Class<?> getPropertyType(String pn) {
            return null;
        }

        @Override
        public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException {
            throw new NoSuchPropertyException("Property [" + pn + "] not found.");
        }

        @Override
        public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException {
            throw new NoSuchPropertyException("Property [" + pn + "] not found.");
        }

        @Override
        public boolean hasProperty(String name) {
            return false;
        }

        @Override
        public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args) throws NoSuchMethodException {
            if ("getClass".equals(mn)) {
                return instance.getClass();
            }
            if ("hashCode".equals(mn)) {
                return instance.hashCode();
            }
            if ("toString".equals(mn)) {
                return instance.toString();
            }
            if ("equals".equals(mn)) {
                if (args.length == 1) {
                    return instance.equals(args[0]);
                }
                throw new IllegalArgumentException("Invoke method [" + mn + "] argument number error.");
            }
            throw new NoSuchMethodException("Method [" + mn + "] not found.");
        }
    };
    private static AtomicLong WRAPPER_CLASS_COUNTER = new AtomicLong(0);

    /**
	 * 获取Wrapper
     * @param c Class instance.
     * @return Wrapper instance(not null).
     */
    public static Wrapper getWrapper(Class<?> c) {
        while (ClassGenerator.isDynamicClass(c)) // can not wrapper on dynamic class.
        {
            c = c.getSuperclass();
        }

        if (c == Object.class) {
            return OBJECT_WRAPPER;
        }
		// 从缓存中获取对应类型的Wrapper
        Wrapper ret = WRAPPER_MAP.get(c);
        if (ret == null) {
			// 如果缓存中没有，就创建一个对应类型的Wrapper
            ret = makeWrapper(c);
			// 使用接口类型作为key避免重复，因为Wrapper的命名是Wrapper0-WrapperN
            WRAPPER_MAP.put(c, ret);
        }
        return ret;
    }

	/**
	 * 创建指定类型的Wrapper
	 */
	private static Wrapper makeWrapper(Class<?> c) {
		// 不会为私有类创建Wrapper
		if (c.isPrimitive()) {
			throw new IllegalArgumentException("Can not create wrapper for primitive type: " + c);
		}
		// 获取类的名称和类加载器
		String name = c.getName();
		ClassLoader cl = ClassUtils.getClassLoader(c);
		// 设置属性方法的开头方法
		StringBuilder c1 = new StringBuilder("public void setPropertyValue(Object o, String n, Object v){ ");
		// 调用获取属性方法的开头方法
		StringBuilder c2 = new StringBuilder("public Object getPropertyValue(Object o, String n){ ");
		// 调用invokeMethod()的开头方法
		StringBuilder c3 = new StringBuilder("public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws " + InvocationTargetException.class.getName() + "{ ");

		c1.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
		// public void setPropertyValue(Object o, String n, Object v){ com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService w;
		// try{ w = ((com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService)$1); }
		// catch(Throwable e){ throw new IllegalArgumentException(e); }
		c2.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
		// public Object getPropertyValue(Object o, String n){ com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService w;
		// try{ w = ((com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService)$1); }
		// catch(Throwable e){ throw new IllegalArgumentException(e); }
		c3.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
		// public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService w;
		// try{ w = ((com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService)$1); }
		// catch(Throwable e){ throw new IllegalArgumentException(e); }

		Map<String, Class<?>> pts = new HashMap<>(); // <property name, property types>
		Map<String, Method> ms = new LinkedHashMap<>(); // <method desc, Method instance>
		List<String> mns = new ArrayList<>(); // method names.
		List<String> dmns = new ArrayList<>(); // declaring method names.

		// 获取所有的public字段
		for (Field f : c.getFields()) {
			// 获取字段的名称
			String fn = f.getName();
			// 获取字段的类型
			Class<?> ft = f.getType();
			// 不处理static和transient字段
			if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) {
				continue;
			}
			// 添加解析的字段
			c1.append(" if( $2.equals(\"").append(fn).append("\") ){ w.").append(fn).append("=").append(arg(ft, "$3")).append("; return; }");
			c2.append(" if( $2.equals(\"").append(fn).append("\") ){ return ($w)w.").append(fn).append("; }");
			pts.put(fn, ft);
		}

		Method[] methods = c.getMethods();
		// 获取所有的public方法
		boolean hasMethod = hasMethods(methods);
		if (hasMethod) {
			c3.append(" try{");
			// 遍历所有的public方法
			for (Method m : methods) {
				// 不处理返回Object的方法，大部分都是内置的方法
				if (m.getDeclaringClass() == Object.class) {
					continue;
				}
				// 获取方法名称
				String mn = m.getName();
				c3.append(" if( \"").append(mn).append("\".equals( $2 ) ");
				int len = m.getParameterTypes().length;
				// 为了区分是不是重载，不能仅以方法名进行区分，要以方法名+参数类型长度区分
				c3.append(" && ").append(" $3.length == ").append(len);
				// 如果存在方法重名的情况
				boolean override = false;
				for (Method m2 : methods) {
					if (m != m2 && m.getName().equals(m2.getName())) {
						override = true;
						break;
					}
				}
				// 需要对参数类型名称进行比较
				if (override) {
					if (len > 0) {
						for (int l = 0; l < len; l++) {
							c3.append(" && ").append(" $3[").append(l).append("].getName().equals(\"")
									.append(m.getParameterTypes()[l].getName()).append("\")");
						}
					}
				}

				c3.append(" ) { ");
				// 如果
				if (m.getReturnType() == Void.TYPE) {
					c3.append(" w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");").append(" return null;");
				} else {
					c3.append(" return ($w)w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");");
				}

				c3.append(" }");

				mns.add(mn);
				// 如果方法不是继承下来的，则添加到declaringClass集合中
				if (m.getDeclaringClass() == c) {
					dmns.add(mn);
				}
				// 添加方法的描述，描述是Dubbo自己构建的
				ms.put(ReflectUtils.getDesc(m), m);
			}
			// 如果有方法，添加tryCatch
			c3.append(" } catch(Throwable e) { ");
			c3.append("     throw new java.lang.reflect.InvocationTargetException(e); ");
			c3.append(" }");
		}
		// 添加invokeMethod()未匹配到的方法
		c3.append(" throw new " + NoSuchMethodException.class.getName() + "(\"Not found method \\\"\"+$2+\"\\\" in class " + c.getName() + ".\"); }");

		// 处理getter、setter方法
		Matcher matcher;
		for (Map.Entry<String, Method> entry : ms.entrySet()) {
			String md = entry.getKey();
			Method method = entry.getValue();
			// 如果是getter方法
			if ((matcher = ReflectUtils.GETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
				String pn = propertyName(matcher.group(1));
				c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName()).append("(); }");
				pts.put(pn, method.getReturnType());
			} else if ((matcher = ReflectUtils.IS_HAS_CAN_METHOD_DESC_PATTERN.matcher(md)).matches()) {
				// 如果是以is|can|has为前缀的方法
				String pn = propertyName(matcher.group(1));
				c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName()).append("(); }");
				pts.put(pn, method.getReturnType());
			} else if ((matcher = ReflectUtils.SETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
				// 如果是setter方法
				Class<?> pt = method.getParameterTypes()[0];
				String pn = propertyName(matcher.group(1));
				c1.append(" if( $2.equals(\"").append(pn).append("\") ){ w.").append(method.getName()).append("(").append(arg(pt, "$3")).append("); return; }");
				pts.put(pn, pt);
			}
			// 均添加到PTS中
		}
		// 上面添加的c3的异常处理，这里负责c1、c2的异常处理
		c1.append(" throw new " + NoSuchPropertyException.class.getName() + "(\"Not found property \\\"\"+$2+\"\\\" field or setter method in class " + c.getName() + ".\"); }");
		c2.append(" throw new " + NoSuchPropertyException.class.getName() + "(\"Not found property \\\"\"+$2+\"\\\" field or setter method in class " + c.getName() + ".\"); }");
		// public void setPropertyValue(Object o, String n, Object v){ com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService w;
		// try{ w = ((com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }
		// throw new org.apache.dubbo.common.bytecode.NoSuchPropertyException("Not found property \""+$2+"\" field or setter method in class com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService."); }

		// public Object getPropertyValue(Object o, String n){ com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService w;
		// try{ w = ((com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService)$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }
		// throw new org.apache.dubbo.common.bytecode.NoSuchPropertyException("Not found property \""+$2+"\" field or setter method in class com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService."); }

		// public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException{ com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService w;
		// try{
		// 		w = ((com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService)$1);
		// }catch(Throwable e){ throw new IllegalArgumentException(e); }
		// try{ if( "shine".equals( $2 )  &&  $3.length == 0 ) {  return ($w)w.shine(); } } catch(Throwable e) {      throw new java.lang.reflect.InvocationTargetException(e);  }
		// throw new org.apache.dubbo.common.bytecode.NoSuchMethodException("Not found method \""+$2+"\" in class com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService."); }
		// 计数器操作
		long id = WRAPPER_CLASS_COUNTER.getAndIncrement();
		// 获取类的生成器
		ClassGenerator cc = ClassGenerator.newInstance(cl);
		// 构造一个Wrapper代理类
		cc.setClassName((Modifier.isPublic(c.getModifiers()) ? Wrapper.class.getName() : c.getName() + "$sw") + id);
		cc.setSuperClass(Wrapper.class);

		cc.addDefaultConstructor();
		cc.addField("public static String[] pns;"); // property name array.
		cc.addField("public static " + Map.class.getName() + " pts;"); // property type map.
		cc.addField("public static String[] mns;"); // all method name array.
		cc.addField("public static String[] dmns;"); // declared method name array.
		for (int i = 0, len = ms.size(); i < len; i++) {
			cc.addField("public static Class[] mts" + i + ";");
		}
		// 添加方法
		cc.addMethod("public String[] getPropertyNames(){ return pns; }");
		cc.addMethod("public boolean hasProperty(String n){ return pts.containsKey($1); }");
		cc.addMethod("public Class getPropertyType(String n){ return (Class)pts.get($1); }");
		cc.addMethod("public String[] getMethodNames(){ return mns; }");
		cc.addMethod("public String[] getDeclaredMethodNames(){ return dmns; }");
		cc.addMethod(c1.toString());
		cc.addMethod(c2.toString());
		cc.addMethod(c3.toString());

		try {
			// 使用类加载器加载类
			Class<?> wc = cc.toClass();
			// 设置静态字段
			wc.getField("pts").set(null, pts);
			wc.getField("pns").set(null, pts.keySet().toArray(new String[0]));
			wc.getField("mns").set(null, mns.toArray(new String[0]));
			wc.getField("dmns").set(null, dmns.toArray(new String[0]));
			int ix = 0;
			for (Method m : ms.values()) {
				wc.getField("mts" + ix++).set(null, m.getParameterTypes());
			}
			// 返回一个Wrapper代理实例
			// 生成的Wrapper类名是Wrapper0，Wrapper1，Wrapper2
			return (Wrapper) wc.newInstance();
		} catch (RuntimeException e) {
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			cc.release();
			ms.clear();
			mns.clear();
			dmns.clear();
		}
	}

    private static String arg(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (cl == Boolean.TYPE) {
                return "((Boolean)" + name + ").booleanValue()";
            }
            if (cl == Byte.TYPE) {
                return "((Byte)" + name + ").byteValue()";
            }
            if (cl == Character.TYPE) {
                return "((Character)" + name + ").charValue()";
            }
            if (cl == Double.TYPE) {
                return "((Number)" + name + ").doubleValue()";
            }
            if (cl == Float.TYPE) {
                return "((Number)" + name + ").floatValue()";
            }
            if (cl == Integer.TYPE) {
                return "((Number)" + name + ").intValue()";
            }
            if (cl == Long.TYPE) {
                return "((Number)" + name + ").longValue()";
            }
            if (cl == Short.TYPE) {
                return "((Number)" + name + ").shortValue()";
            }
            throw new RuntimeException("Unknown primitive type: " + cl.getName());
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    private static String args(Class<?>[] cs, String name) {
        int len = cs.length;
        if (len == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(arg(cs[i], name + "[" + i + "]"));
        }
        return sb.toString();
    }

    private static String propertyName(String pn) {
        return pn.length() == 1 || Character.isLowerCase(pn.charAt(1)) ? Character.toLowerCase(pn.charAt(0)) + pn.substring(1) : pn;
    }

    private static boolean hasMethods(Method[] methods) {
        if (methods == null || methods.length == 0) {
            return false;
        }
        for (Method m : methods) {
            if (m.getDeclaringClass() != Object.class) {
                return true;
            }
        }
        return false;
    }

    /**
     * get property name array.
     *
     * @return property name array.
     */
    abstract public String[] getPropertyNames();

    /**
     * get property type.
     *
     * @param pn property name.
     * @return Property type or nul.
     */
    abstract public Class<?> getPropertyType(String pn);

    /**
     * has property.
     *
     * @param name property name.
     * @return has or has not.
     */
    abstract public boolean hasProperty(String name);

    /**
     * get property value.
     *
     * @param instance instance.
     * @param pn       property name.
     * @return value.
     */
    abstract public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException, IllegalArgumentException;

    /**
     * set property value.
     *
     * @param instance instance.
     * @param pn       property name.
     * @param pv       property value.
     */
    abstract public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException, IllegalArgumentException;

    /**
     * get property value.
     *
     * @param instance instance.
     * @param pns      property name array.
     * @return value array.
     */
    public Object[] getPropertyValues(Object instance, String[] pns) throws NoSuchPropertyException, IllegalArgumentException {
        Object[] ret = new Object[pns.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = getPropertyValue(instance, pns[i]);
        }
        return ret;
    }

    /**
     * set property value.
     *
     * @param instance instance.
     * @param pns      property name array.
     * @param pvs      property value array.
     */
    public void setPropertyValues(Object instance, String[] pns, Object[] pvs) throws NoSuchPropertyException, IllegalArgumentException {
        if (pns.length != pvs.length) {
            throw new IllegalArgumentException("pns.length != pvs.length");
        }

        for (int i = 0; i < pns.length; i++) {
            setPropertyValue(instance, pns[i], pvs[i]);
        }
    }

    /**
     * get method name array.
     *
     * @return method name array.
     */
    abstract public String[] getMethodNames();

    /**
     * get method name array.
     *
     * @return method name array.
     */
    abstract public String[] getDeclaredMethodNames();

    /**
     * has method.
     *
     * @param name method name.
     * @return has or has not.
     */
    public boolean hasMethod(String name) {
        for (String mn : getMethodNames()) {
            if (mn.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * invoke method.
     *
     * @param instance instance.
     * @param mn       method name.
     * @param types
     * @param args     argument array.
     * @return return value.
     */
    abstract public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args) throws NoSuchMethodException, InvocationTargetException;
}
