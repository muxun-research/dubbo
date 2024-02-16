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

import org.apache.dubbo.common.Extension;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.beans.support.InstantiationStrategy;
import org.apache.dubbo.common.compact.Dubbo2ActivateUtils;
import org.apache.dubbo.common.compact.Dubbo2CompactUtils;
import org.apache.dubbo.common.context.Lifecycle;
import org.apache.dubbo.common.extension.support.ActivateComparator;
import org.apache.dubbo.common.extension.support.WrapperComparator;
import org.apache.dubbo.common.lang.Prioritized;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.resource.Disposable;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.ClassLoaderResourceLoader;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.common.utils.NativeUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelAccessor;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.ref.SoftReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.ServiceLoader.load;
import static java.util.stream.StreamSupport.stream;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOVE_VALUE_PREFIX;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_ERROR_LOAD_EXTENSION;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_LOAD_ENV_VARIABLE;

/**
 * {@link org.apache.dubbo.rpc.model.ApplicationModel}, {@code DubboBootstrap} and this class are
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

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(ExtensionLoader.class);

    private static final Pattern NAME_SEPARATOR = Pattern.compile("\\s*[,]+\\s*");
    private static final String SPECIAL_SPI_PROPERTIES = "special_spi.properties";
    /**
     * 扩展实现类集合
     * key: 扩展实现类
     * value: 扩展对象
     */
    private final ConcurrentMap<Class<?>, Object> extensionInstances = new ConcurrentHashMap<>(64);

    private final Class<?> type;
    /**
     * 对象工厂
     * 用于调用{@link ExtensionLoader#injectExtension(Object)}方法，向扩展对象注入依赖属性
     * 例如：StubProxyFactoryWrapper中有protocol属性
     */
    private final ExtensionInjector injector;
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
    private final Map<String, Object> cachedActivates = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, Set<String>> cachedActivateGroups = Collections.synchronizedMap(new LinkedHashMap<>());

    private final Map<String, String[][]> cachedActivateValues = Collections.synchronizedMap(new LinkedHashMap<>());
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
    private final Map<String, IllegalStateException> exceptions = new ConcurrentHashMap<>();

    private static volatile LoadingStrategy[] strategies = loadLoadingStrategies();

    private static final Map<String, String> specialSPILoadingStrategyMap = getSpecialSPILoadingStrategyMap();

    private static SoftReference<Map<java.net.URL, List<String>>> urlListMapCache =
            new SoftReference<>(new ConcurrentHashMap<>());

    private static final List<String> ignoredInjectMethodsDesc = getIgnoredInjectMethodsDesc();

    /**
     * Record all unacceptable exceptions when using SPI
     */
    private final Set<String> unacceptableExceptions = new ConcurrentHashSet<>();

    private final ExtensionDirector extensionDirector;
    private final List<ExtensionPostProcessor> extensionPostProcessors;
    private InstantiationStrategy instantiationStrategy;
    private final ActivateComparator activateComparator;
    private final ScopeModel scopeModel;
    private final AtomicBoolean destroyed = new AtomicBoolean();

    public static void setLoadingStrategies(LoadingStrategy... strategies) {
        if (ArrayUtils.isNotEmpty(strategies)) {
            ExtensionLoader.strategies = strategies;
        }
    }

    /**
     * Load all {@link Prioritized prioritized} {@link LoadingStrategy Loading Strategies} via {@link ServiceLoader}
     *
     * @return non-null
     * @since 2.7.7
     */
    private static LoadingStrategy[] loadLoadingStrategies() {
        return stream(load(LoadingStrategy.class).spliterator(), false).sorted().toArray(LoadingStrategy[]::new);
    }

    /**
     * some spi are implements by dubbo framework only and scan multi classloaders resources may cause
     * application startup very slow
     *
     * @return
     */
    private static Map<String, String> getSpecialSPILoadingStrategyMap() {
        Map map = new ConcurrentHashMap<>();
        Properties properties = loadProperties(ExtensionLoader.class.getClassLoader(), SPECIAL_SPI_PROPERTIES);
        map.putAll(properties);
        return map;
    }

    /**
     * Get all {@link LoadingStrategy Loading Strategies}
     *
     * @return non-null
     * @see LoadingStrategy
     * @see Prioritized
     * @since 2.7.7
     */
    public static List<LoadingStrategy> getLoadingStrategies() {
        return asList(strategies);
    }

    private static List<String> getIgnoredInjectMethodsDesc() {
        List<String> ignoreInjectMethodsDesc = new ArrayList<>();
        Arrays.stream(ScopeModelAware.class.getMethods())
                .map(ReflectUtils::getDesc)
                .forEach(ignoreInjectMethodsDesc::add);
        Arrays.stream(ExtensionAccessorAware.class.getMethods())
                .map(ReflectUtils::getDesc)
                .forEach(ignoreInjectMethodsDesc::add);
        return ignoreInjectMethodsDesc;
    }

    ExtensionLoader(Class<?> type, ExtensionDirector extensionDirector, ScopeModel scopeModel) {
        this.type = type;
        this.extensionDirector = extensionDirector;
        this.extensionPostProcessors = extensionDirector.getExtensionPostProcessors();
        initInstantiationStrategy();
        this.injector = (type == ExtensionInjector.class
                ? null
                : extensionDirector.getExtensionLoader(ExtensionInjector.class).getAdaptiveExtension());
        this.activateComparator = new ActivateComparator(extensionDirector);
        this.scopeModel = scopeModel;
    }

    private void initInstantiationStrategy() {
        instantiationStrategy = extensionPostProcessors.stream()
                .filter(extensionPostProcessor -> extensionPostProcessor instanceof ScopeModelAccessor)
                .map(extensionPostProcessor -> new InstantiationStrategy((ScopeModelAccessor) extensionPostProcessor))
                .findFirst()
                .orElse(new InstantiationStrategy());
    }

    /**
     * 获取指定接口类型的扩展加载器
     * @see ApplicationModel#getExtensionDirector()
     * @see FrameworkModel#getExtensionDirector()
     * @see ModuleModel#getExtensionDirector()
     * @see ExtensionDirector#getExtensionLoader(java.lang.Class)
     * @deprecated get extension loader from extension director of some module.
     */
    @Deprecated
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        return ApplicationModel.defaultModel().getDefaultModule().getExtensionLoader(type);
    }

    @Deprecated
    public static void resetExtensionLoader(Class type) {}

    public void destroy() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        // destroy raw extension instance
        extensionInstances.forEach((type, instance) -> {
            if (instance instanceof Disposable) {
                Disposable disposable = (Disposable) instance;
                try {
                    disposable.destroy();
                } catch (Exception e) {
                    logger.error(COMMON_ERROR_LOAD_EXTENSION, "", "", "Error destroying extension " + disposable, e);
                }
            }
        });
        extensionInstances.clear();

        // destroy wrapped extension instance
        for (Holder<Object> holder : cachedInstances.values()) {
            Object wrappedInstance = holder.get();
            if (wrappedInstance instanceof Disposable) {
                Disposable disposable = (Disposable) wrappedInstance;
                try {
                    disposable.destroy();
                } catch (Exception e) {
                    logger.error(COMMON_ERROR_LOAD_EXTENSION, "", "", "Error destroying extension " + disposable, e);
                }
            }
        }
        cachedInstances.clear();
    }

    private void checkDestroyed() {
        if (destroyed.get()) {
            throw new IllegalStateException("ExtensionLoader is destroyed: " + type);
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
        getExtensionClasses(); // load class
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
    @SuppressWarnings("deprecation")
    public List<T> getActivateExtension(URL url, String[] values, String group) {
        checkDestroyed();
        // solve the bug of using @SPI's wrapper method to report a null pointer exception.
        Map<Class<?>, T> activateExtensionsMap = new TreeMap<>(activateComparator);
        List<String> names = values == null
                ? new ArrayList<>(0)
                : Arrays.stream(values).map(StringUtils::trim).collect(Collectors.toList());
        Set<String> namesSet = new HashSet<>(names);
        if (!namesSet.contains(REMOVE_VALUE_PREFIX + DEFAULT_KEY)) {
            if (cachedActivateGroups.size() == 0) {
                synchronized (cachedActivateGroups) {
                    // cache all extensions
                    if (cachedActivateGroups.size() == 0) {
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
                            } else if (Dubbo2CompactUtils.isEnabled()
                                    && Dubbo2ActivateUtils.isActivateLoaded()
                                    && Dubbo2ActivateUtils.getActivateClass().isAssignableFrom(activate.getClass())) {
                                activateGroup = Dubbo2ActivateUtils.getGroup((Annotation) activate);
                                activateValue = Dubbo2ActivateUtils.getValue((Annotation) activate);
                            } else {
                                continue;
                            }
                            cachedActivateGroups.put(name, new HashSet<>(Arrays.asList(activateGroup)));

                            String[][] keyPairs = new String[activateValue.length][];
                            for (int i = 0; i < activateValue.length; i++) {
                                if (activateValue[i].contains(":")) {
                                    keyPairs[i] = new String[2];
                                    String[] arr = activateValue[i].split(":");
                                    keyPairs[i][0] = arr[0];
                                    keyPairs[i][1] = arr[1];
                                } else {
                                    keyPairs[i] = new String[1];
                                    keyPairs[i][0] = activateValue[i];
                                }
                            }
                            cachedActivateValues.put(name, keyPairs);
                        }
                    }
                }
            }

            // group名称匹配
            cachedActivateGroups.forEach((name, activateGroup) -> {
                if (isMatchGroup(group, activateGroup)
                        // 名称匹配
                        && !namesSet.contains(name)
                        && !namesSet.contains(REMOVE_VALUE_PREFIX + name)
                        // 扩展是否生效
                        && isActive(cachedActivateValues.get(name), url)) {

                    activateExtensionsMap.put(getExtensionClass(name), getExtension(name));
                }
            });
        }

        if (namesSet.contains(DEFAULT_KEY)) {
            // will affect order
            // `ext1,default,ext2` means ext1 will happens before all of the default extensions while ext2 will after
            // them
            ArrayList<T> extensionsResult = new ArrayList<>(activateExtensionsMap.size() + names.size());
            for (String name : names) {
                if (name.startsWith(REMOVE_VALUE_PREFIX) || namesSet.contains(REMOVE_VALUE_PREFIX + name)) {
                    continue;
                }
                if (DEFAULT_KEY.equals(name)) {
                    extensionsResult.addAll(activateExtensionsMap.values());
                    continue;
                }
                if (containsExtension(name)) {
                    extensionsResult.add(getExtension(name));
                }
            }
            return extensionsResult;
        } else {
            // add extensions, will be sorted by its order
            for (String name : names) {
                if (name.startsWith(REMOVE_VALUE_PREFIX) || namesSet.contains(REMOVE_VALUE_PREFIX + name)) {
                    continue;
                }
                if (DEFAULT_KEY.equals(name)) {
                    continue;
                }
                if (containsExtension(name)) {
                    activateExtensionsMap.put(getExtensionClass(name), getExtension(name));
                }
            }
            return new ArrayList<>(activateExtensionsMap.values());
        }
    }

    public List<T> getActivateExtensions() {
        checkDestroyed();
        List<T> activateExtensions = new ArrayList<>();
        TreeMap<Class<?>, T> activateExtensionsMap = new TreeMap<>(activateComparator);
        getExtensionClasses();
        for (Map.Entry<String, Object> entry : cachedActivates.entrySet()) {
            String name = entry.getKey();
            Object activate = entry.getValue();
            if (!(activate instanceof Activate)) {
                continue;
            }
            activateExtensionsMap.put(getExtensionClass(name), getExtension(name));
        }
        if (!activateExtensionsMap.isEmpty()) {
            activateExtensions.addAll(activateExtensionsMap.values());
        }

        return activateExtensions;
    }

    private boolean isMatchGroup(String group, Set<String> groups) {
        if (StringUtils.isEmpty(group)) {
            return true;
        }
        if (CollectionUtils.isNotEmpty(groups)) {
            return groups.contains(group);
        }
        return false;
    }

    /**
     * 判断扩展是否生效
     */
    private boolean isActive(String[][] keyPairs, URL url) {
        // 如果@Activate注解value没有赋值，代表注解是生效的
        if (keyPairs.length == 0) {
            return true;
        }
        // 遍历找到URL中是否存在参数@Activate.value，并且参数值非空
        for (String[] keyPair : keyPairs) {
            // @Active(value="key1:value1, key2:value2")
            String key;
            String keyValue = null;
            if (keyPair.length > 1) {
                key = keyPair[0];
                keyValue = keyPair[1];
            } else {
                key = keyPair[0];
            }

            String realValue = url.getParameter(key);
            if (StringUtils.isEmpty(realValue)) {
                realValue = url.getAnyMethodParameter(key);
            }
            if ((keyValue != null && keyValue.equals(realValue))
                    || (keyValue == null && ConfigUtils.isNotEmpty(realValue))) {
                return true;
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
        checkDestroyed();
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

    @SuppressWarnings("unchecked")
    public List<T> getLoadedExtensionInstances() {
        checkDestroyed();
        List<T> instances = new ArrayList<>();
        cachedInstances.values().forEach(holder -> instances.add((T) holder.get()));
        return instances;
    }

    /**
     * 使用指定的名称获取扩展类
     * 没有找到指定扩展名称，会抛出异常
     * @throws IllegalStateException If the specified extension is not found.
     */
    public T getExtension(String name) {
        T extension = getExtension(name, true);
        if (extension == null) {
            throw new IllegalArgumentException("Not find extension: " + name);
        }
        return extension;
    }

    @SuppressWarnings("unchecked")
    public T getExtension(String name, boolean wrap) {
        checkDestroyed();
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }
        if ("true".equals(name)) {
			// 获取默认的扩展类
            return getDefaultExtension();
        }
        String cacheKey = name;
        if (!wrap) {
            cacheKey += "_origin";
        }
        // 获取或者创建Holder
        final Holder<Object> holder = getOrCreateHolder(cacheKey);
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    // 如果holder不存在，代表是新创建的holder，我们为这个holder生成一个真实值
                    instance = createExtension(name, wrap);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    /**
     * 配置默认扩展类的情况下，返回默认扩展类对象
     * 没有配置默认扩展类对象，返回null
     * @param name the name of extension
     * @return non-null
     */
    public T getOrDefaultExtension(String name) {
        return containsExtension(name) ? getExtension(name) : getDefaultExtension();
    }

    /**
     * Return default extension, return <code>null</code> if it's not configured.
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
        checkDestroyed();
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
        checkDestroyed();
        Map<String, Class<?>> classes = getExtensionClasses();
        return Collections.unmodifiableSet(new TreeSet<>(classes.keySet()));
    }

    public Set<T> getSupportedExtensionInstances() {
        checkDestroyed();
        List<T> instances = new LinkedList<>();
        Set<String> supportedExtensions = getSupportedExtensions();
        if (CollectionUtils.isNotEmpty(supportedExtensions)) {
            for (String name : supportedExtensions) {
                instances.add(getExtension(name));
            }
        }
        // sort the Prioritized instances
        instances.sort(Prioritized.COMPARATOR);
        return new LinkedHashSet<>(instances);
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
        checkDestroyed();
        // 首先看是否需要全量加载扩展类
        getExtensionClasses(); // load classes
		// 添加的扩展类型，必须是当前扩展加载器的子类或者同种类型
        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " + clazz + " doesn't implement the Extension " + type);
        }
        // 加载的扩展类不能是接口
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " + clazz + " can't be interface!");
        }
        // 扩展类没有使用@Adaptive注解的情况下，进行添加
        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
			}
			// 如果已经加载，则会抛出异常
            if (cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " + name + " already exists (Extension " + type + ")!");
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
        checkDestroyed();
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " + clazz + " doesn't implement Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " + clazz + " can't be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (!cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " + name + " doesn't exist (Extension " + type + ")!");
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
        checkDestroyed();
        // 判断缓存中是否有实例
        Object instance = cachedAdaptiveInstance.get();
        if (instance == null) {
            // 抛出在创建createAdaptiveInstance时发生的异常
            if (createAdaptiveInstanceError != null) {
                throw new IllegalStateException(
                        "Failed to create adaptive instance: " + createAdaptiveInstanceError.toString(),
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
    private IllegalStateException findException(String name) {
        StringBuilder buf = new StringBuilder("No such extension " + type.getName() + " by name " + name);

        int i = 1;
        // 从加载时发生的异常中，看是否能获取对应名称的异常
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (entry.getKey().toLowerCase().startsWith(name.toLowerCase())) {
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
        }

        if (i == 1) {
            buf.append(", no related exception was found, please check whether related SPI module is missing.");
        }
        return new IllegalStateException(buf.toString());
    }

    /**
     * 创建扩展类对象
     */
    @SuppressWarnings("unchecked")
    private T createExtension(String name, boolean wrap) {
        // 从全部扩展中获取指定扩展名称的扩展类
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz == null || unacceptableExceptions.contains(name)) {
            // 校验，并抛出异常
            throw findException(name);
        }
        try {
            // 根据扩展类，从缓存中获取扩展类的对象实例
            T instance = (T) extensionInstances.get(clazz);
            if (instance == null) {
                // 如果实例不存在，则创建一个实例
                extensionInstances.putIfAbsent(clazz, createExtensionInstance(clazz));
                instance = (T) extensionInstances.get(clazz);
                instance = postProcessBeforeInitialization(instance, name);
                // 注入扩展类依赖的属性
                injectExtension(instance);
                instance = postProcessAfterInitialization(instance, name);
            }

            if (wrap) {
                // 获取使用Wrapper包装的扩展类集合
                List<Class<?>> wrapperClassesList = new ArrayList<>();
                if (cachedWrapperClasses != null) {
                    wrapperClassesList.addAll(cachedWrapperClasses);
                    wrapperClassesList.sort(WrapperComparator.COMPARATOR);
                    Collections.reverse(wrapperClassesList);
                }

                if (CollectionUtils.isNotEmpty(wrapperClassesList)) {
                    for (Class<?> wrapperClass : wrapperClassesList) {
                        Wrapper wrapper = wrapperClass.getAnnotation(Wrapper.class);
                        boolean match = (wrapper == null)
                                || ((ArrayUtils.isEmpty(wrapper.matches())
                                                || ArrayUtils.contains(wrapper.matches(), name))
                                        && !ArrayUtils.contains(wrapper.mismatches(), name));
                        if (match) {
                            // 注入Wrapper对象实例
                            instance = injectExtension(
                                    (T) wrapperClass.getConstructor(type).newInstance(instance));
                            instance = postProcessAfterInitialization(instance, name);
                        }
                    }
                }
            }

            // Warning: After an instance of Lifecycle is wrapped by cachedWrapperClasses, it may not still be Lifecycle
            // instance, this application may not invoke the lifecycle.initialize hook.
            // 返回创建的扩展类对象
            initExtension(instance);
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException(
                    "Extension instance (name: " + name + ", class: " + type + ") couldn't be instantiated: "
                            + t.getMessage(),
                    t);
        }
    }

    private Object createExtensionInstance(Class<?> type) throws ReflectiveOperationException {
        return instantiationStrategy.instantiate(type);
    }

    @SuppressWarnings("unchecked")
    private T postProcessBeforeInitialization(T instance, String name) throws Exception {
        if (extensionPostProcessors != null) {
            for (ExtensionPostProcessor processor : extensionPostProcessors) {
                instance = (T) processor.postProcessBeforeInitialization(instance, name);
            }
        }
        return instance;
    }

    @SuppressWarnings("unchecked")
    private T postProcessAfterInitialization(T instance, String name) throws Exception {
        if (instance instanceof ExtensionAccessorAware) {
            ((ExtensionAccessorAware) instance).setExtensionAccessor(extensionDirector);
        }
        if (extensionPostProcessors != null) {
            for (ExtensionPostProcessor processor : extensionPostProcessors) {
                instance = (T) processor.postProcessAfterInitialization(instance, name);
            }
        }
        return instance;
    }

    private boolean containsExtension(String name) {
        return getExtensionClasses().containsKey(name);
    }

    /**
     * 注入依赖的属性
     */
    private T injectExtension(T instance) {
        if (injector == null) {
            return instance;
        }

        try {
            for (Method method : instance.getClass().getMethods()) {
                if (!isSetter(method)) {
                    continue;
                }
                /**
                 * Check {@link DisableInject} to see if we need auto-injection for this property
                 */
                // 如果扩展属于无需注入类型，不进行注入
                if (method.isAnnotationPresent(DisableInject.class)) {
                    continue;
                }

                // When spiXXX implements ScopeModelAware, ExtensionAccessorAware,
                // the setXXX of ScopeModelAware and ExtensionAccessorAware does not need to be injected
                // 既不是setter方法，也没有使用@DisableInject注解
                // 获取方法的第一个参数类型
                if (method.getDeclaringClass() == ScopeModelAware.class) {
                    continue;
                }
                if (instance instanceof ScopeModelAware || instance instanceof ExtensionAccessorAware) {
                    if (ignoredInjectMethodsDesc.contains(ReflectUtils.getDesc(method))) {
                        continue;
                    }
                }

                Class<?> pt = method.getParameterTypes()[0];
                if (ReflectUtils.isPrimitives(pt)) {
                    continue;
                }
                // 如果第一个参数类型是原始类型
                try {
                    // 获取这个方法需要setter的属性
                    String property = getSetterProperty(method);
                    // 获取属性对象
                    Object object = injector.getInstance(pt, property);
                    // 设置属性值
                    if (object != null) {
                        method.invoke(instance, object);
                    }
                } catch (Exception e) {
                    logger.error(
                            COMMON_ERROR_LOAD_EXTENSION,
                            "",
                            "",
                            "Failed to inject via method " + method.getName() + " of interface " + type.getName() + ": "
                                    + e.getMessage(),
                            e);
                }
            }
        } catch (Exception e) {
            logger.error(COMMON_ERROR_LOAD_EXTENSION, "", "", e.getMessage(), e);
        }
        return instance;
    }

    private void initExtension(T instance) {
        if (instance instanceof Lifecycle) {
            Lifecycle lifecycle = (Lifecycle) instance;
            lifecycle.initialize();
        }
    }

    /**
     * get properties name for setter, for instance: setVersion, return "version"
     * <p>
     * return "", if setter name with length less than 3
     */
    private String getSetterProperty(Method method) {
        return method.getName().length() > 3
                ? method.getName().substring(3, 4).toLowerCase()
                        + method.getName().substring(4)
                : "";
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
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                // 如果没有对应的缓存，加持双重校验锁的情况下，进行重新加载
                if (classes == null) {
                    try {
                        classes = loadExtensionClasses();
                    } catch (InterruptedException e) {
                        logger.error(
                                COMMON_ERROR_LOAD_EXTENSION,
                                "",
                                "",
                                "Exception occurred when loading extension class (interface: " + type + ")",
                                e);
                        throw new IllegalStateException(
                                "Exception occurred when loading extension class (interface: " + type + ")", e);
                    }
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
    }

    /**
     * 在getExtensionClasses()中已经加锁
     */
    @SuppressWarnings("deprecation")
    private Map<String, Class<?>> loadExtensionClasses() throws InterruptedException {
        checkDestroyed();
        cacheDefaultExtensionName();

        Map<String, Class<?>> extensionClasses = new HashMap<>();
        // 扫描下面三个路径
        for (LoadingStrategy strategy : strategies) {
            loadDirectory(extensionClasses, strategy, type.getName());

            // compatible with old ExtensionFactory
            if (this.type == ExtensionInjector.class) {
                loadDirectory(extensionClasses, strategy, ExtensionFactory.class.getName());
            }
        }

        return extensionClasses;
    }

    private void loadDirectory(Map<String, Class<?>> extensionClasses, LoadingStrategy strategy, String type)
            throws InterruptedException {
        loadDirectoryInternal(extensionClasses, strategy, type);
        if (Dubbo2CompactUtils.isEnabled()) {
            try {
                String oldType = type.replace("org.apache", "com.alibaba");
                if (oldType.equals(type)) {
                    return;
                }
                // if class not found,skip try to load resources
                ClassUtils.forName(oldType);
                loadDirectoryInternal(extensionClasses, strategy, oldType);
            } catch (ClassNotFoundException classNotFoundException) {

            }
        }
    }

    /**
     * 存默认的扩展名称
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
    private void loadDirectoryInternal(
            Map<String, Class<?>> extensionClasses, LoadingStrategy loadingStrategy, String type)
            throws InterruptedException {
        String fileName = loadingStrategy.directory() + type;
        try {
            List<ClassLoader> classLoadersToLoad = new LinkedList<>();

            // 在当前环境能找到类加载器的情况下，使用当前环境的类加载器
            if (loadingStrategy.preferExtensionClassLoader()) {
                ClassLoader extensionLoaderClassLoader = ExtensionLoader.class.getClassLoader();
                if (ClassLoader.getSystemClassLoader() != extensionLoaderClassLoader) {
                    classLoadersToLoad.add(extensionLoaderClassLoader);
                }
            }

            if (specialSPILoadingStrategyMap.containsKey(type)) {
                String internalDirectoryType = specialSPILoadingStrategyMap.get(type);
                // skip to load spi when name don't match
                if (!LoadingStrategy.ALL.equals(internalDirectoryType)
                        && !internalDirectoryType.equals(loadingStrategy.getName())) {
                    return;
                }
                classLoadersToLoad.clear();
                classLoadersToLoad.add(ExtensionLoader.class.getClassLoader());
            } else {
                // load from scope model
                Set<ClassLoader> classLoaders = scopeModel.getClassLoaders();

                if (CollectionUtils.isEmpty(classLoaders)) {
                    Enumeration<java.net.URL> resources = ClassLoader.getSystemResources(fileName);
                    if (resources != null) {
                        while (resources.hasMoreElements()) {
                            loadResource(
                                    extensionClasses,
                                    null,
                                    resources.nextElement(),
                                    loadingStrategy.overridden(),
                                    loadingStrategy.includedPackages(),
                                    loadingStrategy.excludedPackages(),
                                    loadingStrategy.onlyExtensionClassLoaderPackages());
                        }
                    }
                } else {
                    classLoadersToLoad.addAll(classLoaders);
                }
            }

            Map<ClassLoader, Set<java.net.URL>> resources =
                    ClassLoaderResourceLoader.loadResources(fileName, classLoadersToLoad);
            resources.forEach(((classLoader, urls) -> {
                loadFromClass(
                        extensionClasses,
                        loadingStrategy.overridden(),
                        urls,
                        classLoader,
                        loadingStrategy.includedPackages(),
                        loadingStrategy.excludedPackages(),
                        loadingStrategy.onlyExtensionClassLoaderPackages());
            }));
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable t) {
            logger.error(
                    COMMON_ERROR_LOAD_EXTENSION,
                    "",
                    "",
                    "Exception occurred when loading extension class (interface: " + type + ", description file: "
                            + fileName + ").",
                    t);
        }
    }

    private void loadFromClass(
            Map<String, Class<?>> extensionClasses,
            boolean overridden,
            Set<java.net.URL> urls,
            ClassLoader classLoader,
            String[] includedPackages,
            String[] excludedPackages,
            String[] onlyExtensionClassLoaderPackages) {
        if (CollectionUtils.isNotEmpty(urls)) {
            for (java.net.URL url : urls) {
                loadResource(
                        extensionClasses,
                        classLoader,
                        url,
                        overridden,
                        includedPackages,
                        excludedPackages,
                        onlyExtensionClassLoaderPackages);
            }
        }
    }

    /**
     * 加载每个地址路径的资源
     */
    private void loadResource(
            Map<String, Class<?>> extensionClasses,
            ClassLoader classLoader,
            java.net.URL resourceURL,
            boolean overridden,
            String[] includedPackages,
            String[] excludedPackages,
            String[] onlyExtensionClassLoaderPackages) {
        try {
            List<String> newContentList = getResourceContent(resourceURL);
            String clazz;
            for (String line : newContentList) {
                try {
                    String name = null;
                    int i = line.indexOf('=');
                    if (i > 0) {
                        name = line.substring(0, i).trim();
                        clazz = line.substring(i + 1).trim();
                    } else {
                        clazz = line;
                    }
                    if (StringUtils.isNotEmpty(clazz)
                            && !isExcluded(clazz, excludedPackages)
                            && isIncluded(clazz, includedPackages)
                            && !isExcludedByClassLoader(clazz, classLoader, onlyExtensionClassLoaderPackages)) {

                        loadClass(
                                classLoader,
                                extensionClasses,
                                resourceURL,
                                Class.forName(clazz, true, classLoader),
                                name,
                                overridden);
                    }
                } catch (Throwable t) {
                    IllegalStateException e = new IllegalStateException(
                            "Failed to load extension class (interface: " + type + ", class line: " + line + ") in "
                                    + resourceURL + ", cause: " + t.getMessage(),
                            t);
                    exceptions.put(line, e);
                }
            }
        } catch (Throwable t) {
            logger.error(
                    COMMON_ERROR_LOAD_EXTENSION,
                    "",
                    "",
                    "Exception occurred when loading extension class (interface: " + type + ", class file: "
                            + resourceURL + ") in " + resourceURL,
                    t);
        }
    }

    private List<String> getResourceContent(java.net.URL resourceURL) throws IOException {
        Map<java.net.URL, List<String>> urlListMap = urlListMapCache.get();
        if (urlListMap == null) {
            synchronized (ExtensionLoader.class) {
                if ((urlListMap = urlListMapCache.get()) == null) {
                    urlListMap = new ConcurrentHashMap<>();
                    urlListMapCache = new SoftReference<>(urlListMap);
                }
            }
        }

        List<String> contentList = urlListMap.computeIfAbsent(resourceURL, key -> {
            List<String> newContentList = new ArrayList<>();

            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(resourceURL.openStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    final int ci = line.indexOf('#');
                    if (ci >= 0) {
                        line = line.substring(0, ci);
                    }
                    line = line.trim();
                    if (line.length() > 0) {
                        newContentList.add(line);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            return newContentList;
        });
        return contentList;
    }

    private boolean isIncluded(String className, String... includedPackages) {
        if (includedPackages != null && includedPackages.length > 0) {
            for (String includedPackage : includedPackages) {
                if (className.startsWith(includedPackage + ".")) {
                    // one match, return true
                    return true;
                }
            }
            // none matcher match, return false
            return false;
        }
        // matcher is empty, return true
        return true;
    }

    private boolean isExcluded(String className, String... excludedPackages) {
        if (excludedPackages != null) {
            for (String excludePackage : excludedPackages) {
                if (className.startsWith(excludePackage + ".")) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isExcludedByClassLoader(
            String className, ClassLoader classLoader, String... onlyExtensionClassLoaderPackages) {
        if (onlyExtensionClassLoaderPackages != null) {
            for (String excludePackage : onlyExtensionClassLoaderPackages) {
                if (className.startsWith(excludePackage + ".")) {
                    // if target classLoader is not ExtensionLoader's classLoader should be excluded
                    return !Objects.equals(ExtensionLoader.class.getClassLoader(), classLoader);
                }
            }
        }
        return false;
    }

    /**
     * 加载指定的类
     */
    private void loadClass(
            ClassLoader classLoader,
            Map<String, Class<?>> extensionClasses,
            java.net.URL resourceURL,
            Class<?> clazz,
            String name,
            boolean overridden) {
        // 判断当前类加载器加载的是不是和类加载器加载过得是不是子父关系或者相同Class类型
        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException(
                    "Error occurred when loading extension class (interface: " + type + ", class line: "
                            + clazz.getName() + "), class " + clazz.getName() + " is not subtype of interface.");
        }

        boolean isActive = loadClassIfActive(classLoader, clazz);

        if (!isActive) {
            return;
        }
        // 如果使用了@Adaptive，往cachedAdaptiveClass中写一份缓存
        if (clazz.isAnnotationPresent(Adaptive.class)) {
            cacheAdaptiveClass(clazz, overridden);
        } else if (isWrapperClass(clazz)) {
            // 或是使用Wrapper类型进行了包装
            cacheWrapperClass(clazz);
        } else {
            // 既没有使用@Adaptive注解，也没有使用Wrapper进行包装
            // 获取指定加载类型的默认构造方法
            // 如果没有指定的扩展类名称，使用注解里的value或者类的名称
            if (StringUtils.isEmpty(name)) {
                name = findAnnotationName(clazz);
                if (name.length() == 0) {
                    throw new IllegalStateException("No such extension name for the class " + clazz.getName()
                            + " in the config " + resourceURL);
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
                    saveInExtensionClass(extensionClasses, clazz, n, overridden);
                }
            }
        }
    }

    private boolean loadClassIfActive(ClassLoader classLoader, Class<?> clazz) {
        Activate activate = clazz.getAnnotation(Activate.class);

        if (activate == null) {
            return true;
        }
        String[] onClass = null;

        if (activate instanceof Activate) {
            onClass = ((Activate) activate).onClass();
        } else if (Dubbo2CompactUtils.isEnabled()
                && Dubbo2ActivateUtils.isActivateLoaded()
                && Dubbo2ActivateUtils.getActivateClass().isAssignableFrom(activate.getClass())) {
            onClass = Dubbo2ActivateUtils.getOnClass(activate);
        }

        boolean isActive = true;

        if (null != onClass && onClass.length > 0) {
            isActive = Arrays.stream(onClass)
                    .filter(StringUtils::isNotBlank)
                    .allMatch(className -> ClassUtils.isPresent(className, classLoader));
        }
        return isActive;
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
    private void saveInExtensionClass(
            Map<String, Class<?>> extensionClasses, Class<?> clazz, String name, boolean overridden) {
        Class<?> c = extensionClasses.get(name);
        if (c == null || overridden) {
            extensionClasses.put(name, clazz);
        } else if (c != clazz) {
            // duplicate implementation is unacceptable
            unacceptableExceptions.add(name);
            String duplicateMsg = "Duplicate extension " + type.getName() + " name " + name + " on " + c.getName()
                    + " and " + clazz.getName();
            logger.error(COMMON_ERROR_LOAD_EXTENSION, "", "", duplicateMsg);
            throw new IllegalStateException(duplicateMsg);
        }
    }

	/**
	 * 缓存使用@Activate注解的类
     */
    @SuppressWarnings("deprecation")
    private void cacheActivateClass(Class<?> clazz, String name) {
		// 判断类不是有@Activate注解
        Activate activate = clazz.getAnnotation(Activate.class);
        if (activate != null) {
			// 使用@Activate注解，放入到扩展名称-@Activate集合中
            cachedActivates.put(name, activate);
        } else if (Dubbo2CompactUtils.isEnabled() && Dubbo2ActivateUtils.isActivateLoaded()) {
            // 兼容旧版本的@Activate注解
            Annotation oldActivate = clazz.getAnnotation(Dubbo2ActivateUtils.getActivateClass());
            if (oldActivate != null) {
                cachedActivates.put(name, oldActivate);
            }
        }
    }

	/**
	 * 缓存使用@Adaptive注解的扩展类
     */
    private void cacheAdaptiveClass(Class<?> clazz, boolean overridden) {
        if (cachedAdaptiveClass == null || overridden) {
            cachedAdaptiveClass = clazz;
        } else if (!cachedAdaptiveClass.equals(clazz)) {
            throw new IllegalStateException(
                    "More than 1 adaptive class found: " + cachedAdaptiveClass.getName() + ", " + clazz.getName());
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
    protected boolean isWrapperClass(Class<?> clazz) {
        // 如果这个类，拥有一个指定加载类型为唯一参数的构造方法，就证明是一个wrapper
        Constructor<?>[] constructors = clazz.getConstructors();
        for (Constructor<?> constructor : constructors) {
            if (constructor.getParameterTypes().length == 1 && constructor.getParameterTypes()[0] == type) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取注解的名称
     */
    @SuppressWarnings("deprecation")
    private String findAnnotationName(Class<?> clazz) {
        // 如果使用extension注解，并且注解中给出了value的值
        Extension extension = clazz.getAnnotation(Extension.class);
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
            T instance = (T) getAdaptiveExtensionClass().newInstance();
            instance = postProcessBeforeInitialization(instance, null);
            // 注入对应扩展类的属性
            injectExtension(instance);
            instance = postProcessAfterInitialization(instance, null);
            initExtension(instance);
            return instance;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Can't create adaptive extension " + type + ", cause: " + e.getMessage(), e);
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
        ClassLoader classLoader = type.getClassLoader();
        try {
            if (NativeUtils.isNative()) {
                return classLoader.loadClass(type.getName() + "$Adaptive");
            }
        } catch (Throwable ignore) {

        }
        String code = new AdaptiveClassCodeGenerator(type, cachedDefaultName).generate();
        org.apache.dubbo.common.compiler.Compiler compiler = extensionDirector
                .getExtensionLoader(org.apache.dubbo.common.compiler.Compiler.class)
                .getAdaptiveExtension();
        // 编译这段代码
        return compiler.compile(type, code, classLoader);
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }

    private static Properties loadProperties(ClassLoader classLoader, String resourceName) {
        Properties properties = new Properties();
        if (classLoader != null) {
            try {
                Enumeration<java.net.URL> resources = classLoader.getResources(resourceName);
                while (resources.hasMoreElements()) {
                    java.net.URL url = resources.nextElement();
                    Properties props = loadFromUrl(url);
                    for (Map.Entry<Object, Object> entry : props.entrySet()) {
                        String key = entry.getKey().toString();
                        if (properties.containsKey(key)) {
                            continue;
                        }
                        properties.put(key, entry.getValue().toString());
                    }
                }
            } catch (IOException ex) {
                logger.error(CONFIG_FAILED_LOAD_ENV_VARIABLE, "", "", "load properties failed.", ex);
            }
        }

        return properties;
    }

    private static Properties loadFromUrl(java.net.URL url) {
        Properties properties = new Properties();
        InputStream is = null;
        try {
            is = url.openStream();
            properties.load(is);
        } catch (IOException e) {
            // ignore
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        return properties;
    }
}
