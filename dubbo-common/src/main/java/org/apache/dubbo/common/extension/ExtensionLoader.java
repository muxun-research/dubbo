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
package org.apache.dubbo.common.extension;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.support.ActivateComparator;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOVE_VALUE_PREFIX;

/**
 * 加载扩展点的核心
 * 自动注入依赖扩展点
 * 自动使用Wrapper对扩展点进行包装
 * 默认的扩展点是一个Adaptive实例
 * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">Service Provider in Java 5</a>
 * @see org.apache.dubbo.common.extension.SPI
 * @see org.apache.dubbo.common.extension.Adaptive
 * @see org.apache.dubbo.common.extension.Activate
 */
public class ExtensionLoader<T> {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionLoader.class);
	/**
	 * 放置配置文件
	 * 文件格式，下同：
	 * 扩展名称=扩展实现类的全限定名
	 */
    private static final String SERVICES_DIRECTORY = "META-INF/services/";
	/**
	 * 放置内部提供的扩展实现
	 */
    private static final String DUBBO_DIRECTORY = "META-INF/dubbo/";
	/**
	 * 放置配置文件
	 */
    private static final String DUBBO_INTERNAL_DIRECTORY = DUBBO_DIRECTORY + "internal/";
	/**
	 * 扩展名称的分隔符，默认使用","
	 */
    private static final Pattern NAME_SEPARATOR = Pattern.compile("\\s*[,]+\\s*");
	/**
	 * 扩展点加载器的集合
	 * key: 扩展接口
	 */
    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<>();
	/**
	 * 扩展实现类集合
	 * key: 扩展实现类
	 * value: 扩展对象
	 */
    private static final ConcurrentMap<Class<?>, Object> EXTENSION_INSTANCES = new ConcurrentHashMap<>();
	/**
	 * 扩展接口
	 * 例如：Protocol
	 */
    private final Class<?> type;
	/**
	 * 对象工厂
	 * 用于调用{@link ExtensionLoader#injectExtension(Object)}方法，向扩展对象注入依赖属性
	 * 例如：StubProxyFactoryWrapper中有protocol属性
	 */
    private final ExtensionFactory objectFactory;
	/**
	 * 缓存扩展类和扩展名的映射
	 * 通过{@link ExtensionLoader#loadExtensionClasses()}方法加载
	 */
    private final ConcurrentMap<Class<?>, String> cachedNames = new ConcurrentHashMap<>();
	/**
	 * 缓存的扩展实现类集合
	 * 不包含如下两种类型：
	 * 1. 自适应扩展实现类，例如：AdaptiveExtensionFactory
	 * 2. 带唯一参数为扩展接口的构造方法实现类，或者说扩展Wrapper实现类，例如ProtocolFilterWrapper
	 * 扩展Wrapper实现类，会添加到{@link ExtensionLoader#cachedWrapperClasses}中
	 */
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<>();
	/**
	 * 扩展名称与@Activate的映射
	 */
    private final Map<String, Object> cachedActivates = new ConcurrentHashMap<>();
	/**
	 * 缓存的扩展对象集合
	 * key: 扩展名称
	 * value: 扩展对象
	 * 例如：key: protocol value: DubboProtocol
	 * 通过{@link ExtensionLoader#loadExtensionClasses()}方法加载
	 */
	private final ConcurrentMap<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<>();
	/**
	 * 缓存@Adaptive扩展对象
	 */
	private final Holder<Object> cachedAdaptiveInstance = new Holder<>();
	/**
	 * 缓存@Adaptive扩展对象的类
	 */
	private volatile Class<?> cachedAdaptiveClass = null;
	/**
	 * 缓存的默认扩展名称
	 */
	private String cachedDefaultName;
	/**
	 * 创建{@link ExtensionLoader#cachedAdaptiveInstance}时发生的异常
	 */
	private volatile Throwable createAdaptiveInstanceError;
	/**
	 * 扩展的Wrapper实现集合
	 * 带唯一参数为扩展接口的构造方法的实现类
	 * 通过{@link ExtensionLoader#loadExtensionClasses()}方法加载
	 */
    private Set<Class<?>> cachedWrapperClasses;
	/**
	 * 扩展名称与加载时发生的异常集合
	 * key: 扩展名称
	 * value: 扩展加载时的异常
	 */
    private Map<String, IllegalStateException> exceptions = new ConcurrentHashMap<>();

    private ExtensionLoader(Class<?> type) {
        this.type = type;
        objectFactory = (type == ExtensionFactory.class ? null : ExtensionLoader.getExtensionLoader(ExtensionFactory.class).getAdaptiveExtension());
    }

    private static <T> boolean withExtensionAnnotation(Class<T> type) {
        return type.isAnnotationPresent(SPI.class);
	}

	/**
	 * 获取指定接口类型的扩展加载器
	 */
	@SuppressWarnings("unchecked")
	public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
		// 首先对指定接口类型进行校验
		if (type == null) {
			throw new IllegalArgumentException("Extension type == null");
		}
		if (!type.isInterface()) {
			throw new IllegalArgumentException("Extension type (" + type + ") is not an interface!");
		}
		if (!withExtensionAnnotation(type)) {
			throw new IllegalArgumentException("Extension type (" + type +
					") is not an extension, because it is NOT annotated with @" + SPI.class.getSimpleName() + "!");
		}
		// 先从缓存中获取指定接口类型的扩展加载器
		ExtensionLoader<T> loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
		if (loader == null) {
			// 如果缓存里没有，就初始化一个对应的类加载器，并返回
			EXTENSION_LOADERS.putIfAbsent(type, new ExtensionLoader<T>(type));
			loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
		}
		return loader;
	}

	/**
	 * 重置扩展加载器
	 * 目前仅用于测试目的使用
	 */
    public static void resetExtensionLoader(Class type) {
		// 获取指定类型的扩展加载器
        ExtensionLoader loader = EXTENSION_LOADERS.get(type);
        if (loader != null) {
			// 获取当前扩展加载器已经加载的类，从扩展实现类中移除
            Map<String, Class<?>> classes = loader.getExtensionClasses();
            for (Map.Entry<String, Class<?>> entry : classes.entrySet()) {
                EXTENSION_INSTANCES.remove(entry.getValue());
            }
            classes.clear();
			// 释放加载的类后，从扩展加载缓存集合中删除指定接口类型
            EXTENSION_LOADERS.remove(type);
        }
    }

    private static ClassLoader findClassLoader() {
		// 获取ExtensionLoader的类加载器
        return ClassUtils.getClassLoader(ExtensionLoader.class);
	}

	/**
	 * 根据指定的扩展加载器实例，获取扩展名称
	 */
	public String getExtensionName(T extensionInstance) {
		return getExtensionName(extensionInstance.getClass());
	}

    public String getExtensionName(Class<?> extensionClass) {
		// 获取所有的扩展类，如果还没有开始加载扩展类，就进行一次全量加载
		getExtensionClasses();
        return cachedNames.get(extensionClass);
	}

	/**
	 * 调用getActivateExtension()获取group=null的使用@Activate注解的扩展类
     * @param url url
     * @param key url parameter key which used to get extension point names
     * @return extension list which are activated.
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String, String)
     */
    public List<T> getActivateExtension(URL url, String key) {
        return getActivateExtension(url, key, null);
	}

	/**
	 * 调用getActivateExtension()获取group=null，有多个扩展点名称的带有@Activate注解的扩展类
     * @param url    url
     * @param values extension point names
     * @return extension list which are activated
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String[] values) {
        return getActivateExtension(url, values, null);
	}

	/**
	 * 调用getActivateExtension()获取有多个扩展点名称的带有@Activate注解的扩展类
	 * 一个扩展点名称，通过分隔符分离成数组
     * @param url   url
     * @param key   url parameter key which used to get extension point names
     * @param group group
     * @return extension list which are activated.
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String key, String group) {
		// 获取URL中存储的扩展点的名称
        String value = url.getParameter(key);
        return getActivateExtension(url, StringUtils.isEmpty(value) ? null : COMMA_SPLIT_PATTERN.split(value), group);
	}

	/**
	 * 获取指定扩展点名称，使用@Activate注解的扩展类对象
     * @param url    url
     * @param values extension point names
     * @param group  group
     * @return extension list which are activated
     * @see org.apache.dubbo.common.extension.Activate
     */
    public List<T> getActivateExtension(URL url, String[] values, String group) {
        List<T> exts = new ArrayList<>();
        List<String> names = values == null ? new ArrayList<>(0) : Arrays.asList(values);
        if (!names.contains(REMOVE_VALUE_PREFIX + DEFAULT_KEY)) {
			// 获取加载的extensionClasses，使用注解的也在此过程中放入到了各自的集合中
            getExtensionClasses();
			// 遍历cachedActivates
            for (Map.Entry<String, Object> entry : cachedActivates.entrySet()) {
                String name = entry.getKey();
                Object activate = entry.getValue();

                String[] activateGroup, activateValue;
				// 因为存在两种注解，会分别进行判断
                if (activate instanceof Activate) {
                    activateGroup = ((Activate) activate).group();
                    activateValue = ((Activate) activate).value();
                } else if (activate instanceof com.alibaba.dubbo.common.extension.Activate) {
                    activateGroup = ((com.alibaba.dubbo.common.extension.Activate) activate).group();
                    activateValue = ((com.alibaba.dubbo.common.extension.Activate) activate).value();
                } else {
                    continue;
				}
				// group名称匹配
                if (isMatchGroup(group, activateGroup)
						// 名称匹配
                        && !names.contains(name)
                        && !names.contains(REMOVE_VALUE_PREFIX + name)
						// 扩展是否生效
                        && isActive(activateValue, url)) {
                    exts.add(getExtension(name));
                }
            }
            exts.sort(ActivateComparator.COMPARATOR);
        }
        List<T> usrs = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (!name.startsWith(REMOVE_VALUE_PREFIX)
                    && !names.contains(REMOVE_VALUE_PREFIX + name)) {
                if (DEFAULT_KEY.equals(name)) {
                    if (!usrs.isEmpty()) {
                        exts.addAll(0, usrs);
                        usrs.clear();
                    }
                } else {
                    usrs.add(getExtension(name));
                }
            }
        }
        if (!usrs.isEmpty()) {
            exts.addAll(usrs);
        }
        return exts;
    }

    private boolean isMatchGroup(String group, String[] groups) {
        if (StringUtils.isEmpty(group)) {
            return true;
        }
        if (groups != null && groups.length > 0) {
            for (String g : groups) {
                if (group.equals(g)) {
                    return true;
                }
            }
        }
        return false;
	}

	/**
	 * 判断扩展是否生效
	 */
	private boolean isActive(String[] keys, URL url) {
		// 如果@Activate注解value没有赋值，代表注解是生效的
		if (keys.length == 0) {
			return true;
		}
		// 遍历找到URL中是否存在参数@Activate.value，并且参数值非空
		for (String key : keys) {
			for (Map.Entry<String, String> entry : url.getParameters().entrySet()) {
				String k = entry.getKey();
				String v = entry.getValue();
				if ((k.equals(key) || k.endsWith("." + key))
						&& ConfigUtils.isNotEmpty(v)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 获取已经加载的扩展类对象，可能会返回null
	 * 此方法不会触发扩展类的加载，有就是有，没有就是没有
     * @see #getExtension(String)
     */
    @SuppressWarnings("unchecked")
    public T getLoadedExtension(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }
        Holder<Object> holder = getOrCreateHolder(name);
        return (T) holder.get();
	}

	/**
	 * 获取或者创建存储扩展类实例的Holder
	 */
	private Holder<Object> getOrCreateHolder(String name) {
		// 先从缓存中获取
		Holder<Object> holder = cachedInstances.get(name);
		if (holder == null) {
			// 如果没有找到，创建一个默认的Holder
			cachedInstances.putIfAbsent(name, new Holder<>());
			holder = cachedInstances.get(name);
		}
		return holder;
	}

	/**
	 * 获取已经加载扩展类实例的集合
	 * 为了获取所有的扩展类对象，通常调用getSupportedExtensions()方法
	 * 此方法和getSupportedExtensions()方法类似
     * @see #getSupportedExtensions()
     */
    public Set<String> getLoadedExtensions() {
        return Collections.unmodifiableSet(new TreeSet<>(cachedInstances.keySet()));
    }

    public Object getLoadedAdaptiveExtensionInstances() {
        return cachedAdaptiveInstance.get();
	}

	/**
	 * 使用指定的名称获取扩展类
	 * 没有找到指定扩展名称，会抛出异常
     */
    @SuppressWarnings("unchecked")
    public T getExtension(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }
        if ("true".equals(name)) {
			// 获取默认的扩展类
            return getDefaultExtension();
		}
		// 获取或者创建Holder
        final Holder<Object> holder = getOrCreateHolder(name);
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
					// 如果holder不存在，代表是新创建的holder，我们为这个holder生成一个真实值
                    instance = createExtension(name);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
	}

	/**
	 * 配置默认扩展类的情况下，返回默认扩展类对象
	 * 没有配置默认扩展类对象，返回null
     */
    public T getDefaultExtension() {
		// 获取所有的扩展
        getExtensionClasses();
        if (StringUtils.isBlank(cachedDefaultName) || "true".equals(cachedDefaultName)) {
            return null;
		}
		// 获取默认的缓存扩展名称
        return getExtension(cachedDefaultName);
	}

	/**
	 * 是否包含指定的扩展名称
	 * 从extensionClass中获取
	 */
	public boolean hasExtension(String name) {
		if (StringUtils.isEmpty(name)) {
			throw new IllegalArgumentException("Extension name == null");
		}
		Class<?> c = this.getExtensionClass(name);
		return c != null;
	}

	/**
	 * 获取目前支持的所有的扩展名称
	 */
	public Set<String> getSupportedExtensions() {
		Map<String, Class<?>> clazzes = getExtensionClasses();
		return Collections.unmodifiableSet(new TreeSet<>(clazzes.keySet()));
	}

	/**
	 * 获取默认的扩展名称
	 * 如果没有配置，返回null
     */
    public String getDefaultExtensionName() {
        getExtensionClasses();
        return cachedDefaultName;
	}

	/**
	 * 添加一个扩展
     * @param name  extension name
     * @param clazz extension class
     * @throws IllegalStateException when extension with the same name has already been registered.
     */
    public void addExtension(String name, Class<?> clazz) {
		// 首先看是否需要全量加载扩展类
        getExtensionClasses(); // load classes
		// 添加的扩展类型，必须是当前扩展加载器的子类或者同种类型
        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + " doesn't implement the Extension " + type);
		}
		// 加载的扩展类不能是接口
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + " can't be interface!");
		}
		// 扩展类没有使用@Adaptive注解的情况下，进行添加
        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
			}
			// 如果已经加载，则会抛出异常
            if (cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " already exists (Extension " + type + ")!");
			}
			// 如果没有加载，放入到缓存中
			// 放入到扩展类-扩展名称缓存中
            cachedNames.put(clazz, name);
			// 放入到扩展名称-扩展类缓存中
            cachedClasses.get().put(name, clazz);
		} else {
			// 如果当前扩展加载器已经缓存一个使用@Adaptive注解的类，抛出异常
            if (cachedAdaptiveClass != null) {
                throw new IllegalStateException("Adaptive Extension already exists (Extension " + type + ")!");
			}
			// 如果当前扩展加载器没有缓存使用@Adaptive注解的类，缓存添加的扩展类
            cachedAdaptiveClass = clazz;
        }
	}

	/**
	 * 替换已经存在的扩展类
	 * 已经废弃
     * @param name  extension name
     * @param clazz extension class
     * @throws IllegalStateException when extension to be placed doesn't exist
     * @deprecated not recommended any longer, and use only when test
     */
    @Deprecated
    public void replaceExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + " doesn't implement Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + " can't be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (!cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " doesn't exist (Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
            cachedInstances.remove(name);
        } else {
            if (cachedAdaptiveClass == null) {
                throw new IllegalStateException("Adaptive Extension doesn't exist (Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
            cachedAdaptiveInstance.set(null);
		}
	}

	/**
	 * 取使用@Adaptive注解的扩展类实例
	 */
	@SuppressWarnings("unchecked")
	public T getAdaptiveExtension() {
		// 判断缓存中是否有实例
		Object instance = cachedAdaptiveInstance.get();
		if (instance == null) {
			// 抛出在创建createAdaptiveInstance时发生的异常
			if (createAdaptiveInstanceError != null) {
				throw new IllegalStateException("Failed to create adaptive instance: " +
						createAdaptiveInstanceError.toString(),
						createAdaptiveInstanceError);
			}
			// 加持双重校验锁，创建使用@Adaptice注解的实例，并设置为缓存对象
			synchronized (cachedAdaptiveInstance) {
				instance = cachedAdaptiveInstance.get();
				if (instance == null) {
					try {
						instance = createAdaptiveExtension();
						cachedAdaptiveInstance.set(instance);
					} catch (Throwable t) {
						createAdaptiveInstanceError = t;
						throw new IllegalStateException("Failed to create adaptive instance: " + t.toString(), t);
					}
				}
			}
		}

		return (T) instance;
	}

	/**
	 * 查找指定扩展名称，是否发生了异常
	 */
	private void findException(String name) {
		// 从加载时发生的异常中，看是否能获取对应名称的异常
		for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
			if (entry.getKey().toLowerCase().contains(name.toLowerCase())) {
				throw entry.getValue();
			}
		}
	}

    private IllegalStateException noExtensionException(String name) {
        StringBuilder buf = new StringBuilder("No such extension " + type.getName() + " by name " + name);


        int i = 1;
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (i == 1) {
                buf.append(", possible causes: ");
            }

            buf.append("\r\n(");
            buf.append(i++);
            buf.append(") ");
            buf.append(entry.getKey());
            buf.append(":\r\n");
            buf.append(StringUtils.toString(entry.getValue()));
        }
        return new IllegalStateException(buf.toString());
	}

	/**
	 * 创建扩展类对象
	 */
	@SuppressWarnings("unchecked")
	private T createExtension(String name) {
		// 校验，并抛出异常
		findException(name);
		// 从全部扩展中获取指定扩展名称的扩展类
		Class<?> clazz = getExtensionClasses().get(name);
		if (clazz == null) {
			throw noExtensionException(name);
		}
		try {
			// 根据扩展类，从缓存中获取扩展类的对象实例
			T instance = (T) EXTENSION_INSTANCES.get(clazz);
			if (instance == null) {
				// 如果实例不存在，则创建一个实例
				EXTENSION_INSTANCES.putIfAbsent(clazz, clazz.newInstance());
				instance = (T) EXTENSION_INSTANCES.get(clazz);
			}
			// 注入扩展类依赖的属性
			injectExtension(instance);
			// 获取使用Wrapper包装的扩展类集合
			Set<Class<?>> wrapperClasses = cachedWrapperClasses;
			if (CollectionUtils.isNotEmpty(wrapperClasses)) {
				for (Class<?> wrapperClass : wrapperClasses) {
					// 注入Wrapper对象实例
					instance = injectExtension((T) wrapperClass.getConstructor(type).newInstance(instance));
				}
			}
			// 返回创建的扩展类对象
			return instance;
		} catch (Throwable t) {
			throw new IllegalStateException("Extension instance (name: " + name + ", class: " +
					type + ") couldn't be instantiated: " + t.getMessage(), t);
		}
	}

	/**
	 * 注入依赖的属性
	 */
	private T injectExtension(T instance) {

		if (objectFactory == null) {
			return instance;
		}

		try {
			for (Method method : instance.getClass().getMethods()) {
				if (!isSetter(method)) {
					continue;
				}
				// 如果扩展属于无需注入类型，不进行注入
				if (method.getAnnotation(DisableInject.class) != null) {
					continue;
				}
				// 既不是setter方法，也没有使用@DisableInject注解
				// 获取方法的第一个参数类型
				Class<?> pt = method.getParameterTypes()[0];
				if (ReflectUtils.isPrimitives(pt)) {
					continue;
				}
				// 如果第一个参数类型是原始类型
				try {
					// 获取这个方法需要setter的属性
					String property = getSetterProperty(method);
					// 获取属性对象
					Object object = objectFactory.getExtension(pt, property);
					// 设置属性值
					if (object != null) {
						method.invoke(instance, object);
					}
				} catch (Exception e) {
					logger.error("Failed to inject via method " + method.getName()
							+ " of interface " + type.getName() + ": " + e.getMessage(), e);
				}

			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return instance;
	}

    /**
     * get properties name for setter, for instance: setVersion, return "version"
     * <p>
     * return "", if setter name with length less than 3
     */
    private String getSetterProperty(Method method) {
        return method.getName().length() > 3 ? method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4) : "";
    }

    /**
     * return true if and only if:
     * <p>
     * 1, public
     * <p>
     * 2, name starts with "set"
     * <p>
     * 3, only has one parameter
     */
    private boolean isSetter(Method method) {
        return method.getName().startsWith("set")
                && method.getParameterTypes().length == 1
                && Modifier.isPublic(method.getModifiers());
	}

	/**
	 * 获取指定扩展名称的扩展类类型
	 */
	private Class<?> getExtensionClass(String name) {
		if (type == null) {
			throw new IllegalArgumentException("Extension type == null");
		}
		if (name == null) {
			throw new IllegalArgumentException("Extension name == null");
		}
		return getExtensionClasses().get(name);
	}

	/**
	 * 获取或加载所有的扩展类
	 */
	private Map<String, Class<?>> getExtensionClasses() {
		// 首先获取缓存的扩展类集合
		Map<String, Class<?>> classes = cachedClasses.get();
		// 如果没有对应的缓存，加持双重校验锁的情况下，进行重新加载
		if (classes == null) {
			synchronized (cachedClasses) {
				classes = cachedClasses.get();
				if (classes == null) {
					classes = loadExtensionClasses();
					cachedClasses.set(classes);
				}
			}
		}
		return classes;
	}

	/**
	 * 在getExtensionClasses()中已经加锁
     */
    private Map<String, Class<?>> loadExtensionClasses() {
        cacheDefaultExtensionName();
		// 扫描下面三个路径
        Map<String, Class<?>> extensionClasses = new HashMap<>();
        loadDirectory(extensionClasses, DUBBO_INTERNAL_DIRECTORY, type.getName());
        loadDirectory(extensionClasses, DUBBO_INTERNAL_DIRECTORY, type.getName().replace("org.apache", "com.alibaba"));
        loadDirectory(extensionClasses, DUBBO_DIRECTORY, type.getName());
        loadDirectory(extensionClasses, DUBBO_DIRECTORY, type.getName().replace("org.apache", "com.alibaba"));
        loadDirectory(extensionClasses, SERVICES_DIRECTORY, type.getName());
        loadDirectory(extensionClasses, SERVICES_DIRECTORY, type.getName().replace("org.apache", "com.alibaba"));
        return extensionClasses;
	}

	/**
	 * 缓存默认的扩展名称
     */
    private void cacheDefaultExtensionName() {
		// 扩展类必须使用@SPI注解
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        if (defaultAnnotation == null) {
            return;
		}

		// 获取@SPI注解中的名称
        String value = defaultAnnotation.value();
        if ((value = value.trim()).length() > 0) {
            String[] names = NAME_SEPARATOR.split(value);
            if (names.length > 1) {
                throw new IllegalStateException("More than 1 default extension name on extension " + type.getName()
                        + ": " + Arrays.toString(names));
			}
			// 只允许有一个名称
            if (names.length == 1) {
                cachedDefaultName = names[0];
            }
		}
	}

	/**
	 * 从指定目录加载扩展类
	 */
	private void loadDirectory(Map<String, Class<?>> extensionClasses, String dir, String type) {
		String fileName = dir + type;
		try {
			Enumeration<java.net.URL> urls;
			ClassLoader classLoader = findClassLoader();
			// 在当前环境能找到类加载器的情况下，使用当前环境的类加载器
			if (classLoader != null) {
				urls = classLoader.getResources(fileName);
			} else {
				// 默认使用系统类加载器
				urls = ClassLoader.getSystemResources(fileName);
			}
			// 获取fileName下的地址路径
			if (urls != null) {
				// 遍历地址路径
				while (urls.hasMoreElements()) {
					java.net.URL resourceURL = urls.nextElement();
					// 加载每个地址路径的资源
					loadResource(extensionClasses, classLoader, resourceURL);
				}
			}
		} catch (Throwable t) {
			logger.error("Exception occurred when loading extension class (interface: " +
					type + ", description file: " + fileName + ").", t);
		}
	}

	/**
	 * 加载每个地址路径的资源
	 */
	private void loadResource(Map<String, Class<?>> extensionClasses, ClassLoader classLoader, java.net.URL resourceURL) {
		try {
			// 使用UTF-8的形式，按行进行读取
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceURL.openStream(), StandardCharsets.UTF_8))) {
				String line;
				while ((line = reader.readLine()) != null) {
					// 去掉"#"及之后的字符
					final int ci = line.indexOf('#');
					if (ci >= 0) {
						line = line.substring(0, ci);
					}
					line = line.trim();
					if (line.length() > 0) {
						try {
							String name = null;
							// 截取"="，前面为扩展名称，后面为扩展的全限定名
							int i = line.indexOf('=');
							if (i > 0) {
								name = line.substring(0, i).trim();
								line = line.substring(i + 1).trim();
							}
							if (line.length() > 0) {
								// 截取类名后加载类
								loadClass(extensionClasses, resourceURL, Class.forName(line, true, classLoader), name);
							}
						} catch (Throwable t) {
							IllegalStateException e = new IllegalStateException("Failed to load extension class (interface: " + type + ", class line: " + line + ") in " + resourceURL + ", cause: " + t.getMessage(), t);
							exceptions.put(line, e);
						}
					}
				}
			}
		} catch (Throwable t) {
			logger.error("Exception occurred when loading extension class (interface: " +
					type + ", class file: " + resourceURL + ") in " + resourceURL, t);
		}
	}

	/**
	 * 加载指定的类
	 */
	private void loadClass(Map<String, Class<?>> extensionClasses, java.net.URL resourceURL, Class<?> clazz, String name) throws NoSuchMethodException {
		// 判断当前类加载器加载的是不是和类加载器加载过得是不是子父关系或者相同Class类型
		if (!type.isAssignableFrom(clazz)) {
			throw new IllegalStateException("Error occurred when loading extension class (interface: " +
					type + ", class line: " + clazz.getName() + "), class "
					+ clazz.getName() + " is not subtype of interface.");
		}
		// 如果使用了@Adaptive，往cachedAdaptiveClass中写一份缓存
		if (clazz.isAnnotationPresent(Adaptive.class)) {
			cacheAdaptiveClass(clazz);
		} else if (isWrapperClass(clazz)) {
			// 或是使用Wrapper类型进行了包装
			cacheWrapperClass(clazz);
		} else {
			// 既没有使用@Adaptive注解，也没有使用Wrapper进行包装
			// 获取指定加载类型的默认构造方法
			clazz.getConstructor();
			// 如果没有指定的扩展类名称，使用注解里的value或者类的名称
			if (StringUtils.isEmpty(name)) {
				name = findAnnotationName(clazz);
				if (name.length() == 0) {
					throw new IllegalStateException("No such extension name for the class " + clazz.getName() + " in the config " + resourceURL);
				}
			}
			// 使用","分割name，因为可能存在一行记录多个类
			String[] names = NAME_SEPARATOR.split(name);
			// 有类需要加载
			if (ArrayUtils.isNotEmpty(names)) {
				// 缓存使用@Activate注解的扩展
				cacheActivateClass(clazz, names[0]);
				for (String n : names) {
					// 缓存扩展类和扩展名称集合
					cacheName(clazz, n);
					// 缓存扩展名称和扩展类的集合
					saveInExtensionClass(extensionClasses, clazz, n);
				}
			}
		}
	}

	/**
	 * 缓存扩展类-扩展名称
     */
    private void cacheName(Class<?> clazz, String name) {
        if (!cachedNames.containsKey(clazz)) {
            cachedNames.put(clazz, name);
        }
	}

	/**
	 * 将扩展名称和扩展类放入到extensionClasses中
     */
    private void saveInExtensionClass(Map<String, Class<?>> extensionClasses, Class<?> clazz, String name) {
        Class<?> c = extensionClasses.get(name);
        if (c == null) {
            extensionClasses.put(name, clazz);
        } else if (c != clazz) {
            throw new IllegalStateException("Duplicate extension " + type.getName() + " name " + name + " on " + c.getName() + " and " + clazz.getName());
        }
	}

	/**
	 * 缓存使用@Activate注解的类
     */
    private void cacheActivateClass(Class<?> clazz, String name) {
		// 判断类不是有@Activate注解
        Activate activate = clazz.getAnnotation(Activate.class);
        if (activate != null) {
			// 使用@Activate注解，放入到扩展名称-@Activate集合中
            cachedActivates.put(name, activate);
		} else {
			// 兼容旧版本的@Activate注解
            com.alibaba.dubbo.common.extension.Activate oldActivate = clazz.getAnnotation(com.alibaba.dubbo.common.extension.Activate.class);
            if (oldActivate != null) {
                cachedActivates.put(name, oldActivate);
            }
        }
	}

	/**
	 * 缓存使用@Adaptive注解的扩展类
     */
    private void cacheAdaptiveClass(Class<?> clazz) {
        if (cachedAdaptiveClass == null) {
            cachedAdaptiveClass = clazz;
        } else if (!cachedAdaptiveClass.equals(clazz)) {
            throw new IllegalStateException("More than 1 adaptive class found: "
                    + cachedAdaptiveClass.getName()
                    + ", " + clazz.getName());
        }
	}

	/**
	 * 缓存使用Wrapper包装的扩展类
	 * 比如: ProtocolFilterWrapper, ProtocalListenerWrapper
     */
    private void cacheWrapperClass(Class<?> clazz) {
        if (cachedWrapperClasses == null) {
            cachedWrapperClasses = new ConcurrentHashSet<>();
        }
        cachedWrapperClasses.add(clazz);
	}

	/**
	 * 判断指定类是不是Wrapper类
     */
    private boolean isWrapperClass(Class<?> clazz) {
		try {
			// 如果这个类，拥有一个指定加载类型为唯一参数的构造方法，就证明是一个wrapper
            clazz.getConstructor(type);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
		}
	}

	/**
	 * 获取注解的名称
	 */
	@SuppressWarnings("deprecation")
	private String findAnnotationName(Class<?> clazz) {
		// 如果使用extension注解，并且注解中给出了value的值
		org.apache.dubbo.common.Extension extension = clazz.getAnnotation(org.apache.dubbo.common.Extension.class);
		if (extension != null) {
			return extension.value();
		}
		// 获取类的简单名称
		String name = clazz.getSimpleName();
		// 如果类名是以指定的类型结尾的
		if (name.endsWith(type.getSimpleName())) {
			// 就取指定类型前面的部分
			name = name.substring(0, name.length() - type.getSimpleName().length());
		}
		// 使用小写
		return name.toLowerCase();
	}

	/**
	 * 创建使用@Adaptive注解的扩展类对象
	 */
	@SuppressWarnings("unchecked")
	private T createAdaptiveExtension() {
		try {
			// 注入对应扩展类的属性
			return injectExtension((T) getAdaptiveExtensionClass().newInstance());
		} catch (Exception e) {
			throw new IllegalStateException("Can't create adaptive extension " + type + ", cause: " + e.getMessage(), e);
		}
	}

	/**
	 * 获取使用@Adaptive注解的扩展类
	 */
	private Class<?> getAdaptiveExtensionClass() {
		getExtensionClasses();
		if (cachedAdaptiveClass != null) {
			return cachedAdaptiveClass;
		}
		// 如果没有缓存，则创建一个使用@Adaptive注解的扩展类
		return cachedAdaptiveClass = createAdaptiveExtensionClass();
	}

	/**
	 * 创建一个使用@Adaptive注解的扩展类
	 */
	private Class<?> createAdaptiveExtensionClass() {
		// 生成@Adaptive注解代码的字符串
		String code = new AdaptiveClassCodeGenerator(type, cachedDefaultName).generate();
		ClassLoader classLoader = findClassLoader();
		org.apache.dubbo.common.compiler.Compiler compiler = ExtensionLoader.getExtensionLoader(org.apache.dubbo.common.compiler.Compiler.class).getAdaptiveExtension();
		// 编译这段代码
		return compiler.compile(code, classLoader);
	}

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }

}
