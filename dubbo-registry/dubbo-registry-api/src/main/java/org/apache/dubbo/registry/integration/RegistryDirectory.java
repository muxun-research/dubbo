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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.configcenter.DynamicConfiguration;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DISABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.APP_DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.COMPATIBLE_CONFIG_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTE_PROTOCOL;
import static org.apache.dubbo.registry.Constants.CONFIGURATORS_SUFFIX;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;


/**
 * RegistryDirectory
 */
public class RegistryDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    private static final Cluster CLUSTER = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    private static final RouterFactory ROUTER_FACTORY = ExtensionLoader.getExtensionLoader(RouterFactory.class)
            .getAdaptiveExtension();

    private final String serviceKey; // Initialization at construction time, assertion not null
    private final Class<T> serviceType; // Initialization at construction time, assertion not null
    private final Map<String, String> queryMap; // Initialization at construction time, assertion not null
	/**
	 * 原始的Directory URL
	 */
	private final URL directoryUrl; // Initialization at construction time, assertion not null, and always assign non null value
	/**
	 * 判断是否是多group
	 */
	private final boolean multiGroup;
    private Protocol protocol; // Initialization at the time of injection, the assertion is not null
    private Registry registry; // Initialization at the time of injection, the assertion is not null
    private volatile boolean forbidden = false;
	/**
	 * 复写的URL，结合配置规则
	 */
    private volatile URL overrideDirectoryUrl; // Initialization at construction time, assertion not null, and always assign non null value

    private volatile URL registeredConsumerUrl;

    /**
     * override rules
     * Priority: override>-D>consumer>provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     */
    private volatile List<Configurator> configurators; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Map<url, Invoker> cache service url to invoker mapping.
    private volatile Map<String, Invoker<T>> urlInvokerMap; // The initial value is null and the midway may be assigned to null, please use the local variable reference
    private volatile List<Invoker<T>> invokers;

	// 用于缓存invokerURL，以此来转换为invokers
	// 初始化的值为null，并且在运行期间也有可能被设置为null，请使用本地变量引用
	private volatile Set<URL> cachedInvokerUrls;

    private static final ConsumerConfigurationListener CONSUMER_CONFIGURATION_LISTENER = new ConsumerConfigurationListener();
    private ReferenceConfigurationListener serviceConfigurationListener;


    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(url);
        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }
        if (url.getServiceKey() == null || url.getServiceKey().length() == 0) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }
        this.serviceType = serviceType;
        this.serviceKey = url.getServiceKey();
        this.queryMap = StringUtils.parseQueryString(url.getParameterAndDecoded(REFER_KEY));
        this.overrideDirectoryUrl = this.directoryUrl = turnRegistryUrlToConsumerUrl(url);
        String group = directoryUrl.getParameter(GROUP_KEY, "");
        this.multiGroup = group != null && (ANY_VALUE.equals(group) || group.contains(","));
    }

    private URL turnRegistryUrlToConsumerUrl(URL url) {
        // save any parameter in registry that will be useful to the new url.
        String isDefault = url.getParameter(DEFAULT_KEY);
        if (StringUtils.isNotEmpty(isDefault)) {
            queryMap.put(REGISTRY_KEY + "." + DEFAULT_KEY, isDefault);
        }
        return URLBuilder.from(url)
                .setPath(url.getServiceInterface())
                .clearParameters()
                .addParameters(queryMap)
                .removeParameter(MONITOR_KEY)
                .build();
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
	}

	// 向注册中心发起订阅
	public void subscribe(URL url) {
		// 设置消费者URL
		setConsumerUrl(url);
		// 设置监听者
		CONSUMER_CONFIGURATION_LISTENER.addNotifyListener(this);
		// 创建服务配置监听者
		serviceConfigurationListener = new ReferenceConfigurationListener(this, url);
		// 向注册中心发起订阅
		registry.subscribe(url, this);
	}


    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }

        // unregister.
        try {
            if (getRegisteredConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unregister(getRegisteredConsumerUrl());
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unregister service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        // unsubscribe.
        try {
            if (getConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getConsumerUrl(), this);
            }
            DynamicConfiguration.getDynamicConfiguration()
                    .removeListener(ApplicationModel.getApplication(), CONSUMER_CONFIGURATION_LISTENER);
        } catch (Throwable t) {
            logger.warn("unexpected error when unsubscribe service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        super.destroy(); // must be executed after unsubscribing
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
	}

	/**
	 * 注册中心产生变化时，汇通通知对应的监听者
	 */
	@Override
	public synchronized void notify(List<URL> urls) {
		Map<String, List<URL>> categoryUrls = urls.stream()
				.filter(Objects::nonNull)
				.filter(this::isValidCategory)
				.filter(this::isNotCompatibleFor26x)
				.collect(Collectors.groupingBy(url -> {
					// 配置类
					if (UrlUtils.isConfigurator(url)) {
						return CONFIGURATORS_CATEGORY;
						// 路由类
					} else if (UrlUtils.isRoute(url)) {
						return ROUTERS_CATEGORY;
						// 服务类
					} else if (UrlUtils.isProvider(url)) {
						return PROVIDERS_CATEGORY;
					}
					return "";
				}));
		// 更新配置信息
		List<URL> configuratorURLs = categoryUrls.getOrDefault(CONFIGURATORS_CATEGORY, Collections.emptyList());
		this.configurators = Configurator.toConfigurators(configuratorURLs).orElse(this.configurators);
		// 更新路由信息
		List<URL> routerURLs = categoryUrls.getOrDefault(ROUTERS_CATEGORY, Collections.emptyList());
		// 并添加到routerChain中
		toRouters(routerURLs).ifPresent(this::addRouters);

		// 更新provider信息
		List<URL> providerURLs = categoryUrls.getOrDefault(PROVIDERS_CATEGORY, Collections.emptyList());
		refreshOverrideAndInvoker(providerURLs);
	}

    private void refreshOverrideAndInvoker(List<URL> urls) {
        // mock zookeeper://xxx?mock=return null
        overrideDirectoryUrl();
        refreshInvoker(urls);
    }

	/**
	 * 将invokerURL转换为Invoker Map集合，转换的规则如下：
	 * 如果URL已经转换为invoker，将不会被缓存重新引用，并直接持有，并且通知如果有URL有参数变化，将会进行重新引用
	 * 如果
	 * 如果入参的InvokerURL列表是空的，代表规则仅是一个复写的规则，或者是路由规则，这就需要重新进行对比来决定是否需要重新引用
     * Convert the invokerURL list to the Invoker Map. The rules of the conversion are as follows:
     * <ol>
     * <li> If URL has been converted to invoker, it is no longer re-referenced and obtained directly from the cache,
     * and notice that any parameter changes in the URL will be re-referenced.</li>
     * <li>If the incoming invoker list is not empty, it means that it is the latest invoker list.</li>
     * <li>If the list of incoming invokerUrl is empty, It means that the rule is only a override rule or a route
     * rule, which needs to be re-contrasted to decide whether to re-reference.</li>
     * </ol>
	 *
	 * @param invokerUrls 不能为空
     */
    // TODO: 2017/8/31 FIXME The thread pool should be used to refresh the address, otherwise the task may be accumulated.
    private void refreshInvoker(List<URL> invokerUrls) {
        Assert.notNull(invokerUrls, "invokerUrls should not be null");
		// 如果invokerURL中仅有一个URL，且这个URL的协议是"empty://"
        if (invokerUrls.size() == 1
                && invokerUrls.get(0) != null
                && EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
			// 证明需要进行清除信息，forbidden设为true
			this.forbidden = true;
            this.invokers = Collections.emptyList();
			// 更新routerChain的invoker为空集合
            routerChain.setInvokers(this.invokers);
			// 关闭所有的invoker
			destroyAllInvokers();
        } else {
			// Directory是可访问的
			this.forbidden = false;
			// 本地引用
			Map<String, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap;
            if (invokerUrls == Collections.<URL>emptyList()) {
                invokerUrls = new ArrayList<>();
			}
			// 如果invokerURL为空集合，但是缓存的invokerURLs不为null，证明之前有invoker，现在没有了
			// 判断是否更新invokerUrls缓存
            if (invokerUrls.isEmpty() && this.cachedInvokerUrls != null) {
				// 使用缓存的invokerURL
                invokerUrls.addAll(this.cachedInvokerUrls);
            } else {
				// 更新invokerURLs缓存
                this.cachedInvokerUrls = new HashSet<>();
				this.cachedInvokerUrls.addAll(invokerUrls);
            }
            if (invokerUrls.isEmpty()) {
                return;
			}
			// 将InvokerURL转换为Invoker Map集合
			// 此时理论上invoker map集合中必有invoker
			Map<String, Invoker<T>> newUrlInvokerMap = toInvokers(invokerUrls);

			// 但是如果invoker map集合为空，证明在计算过程中出现了问题，并没有执行完成
			// 可能的出现问题的原因：
			// 1. client配置的协议和server配置的协议是不同的，比如consumer.protocol=dubbo，provider.protocol=rest
			// 2. 注册中心出现了问题，推送了不合常规的数据
			// 直接返回
            if (CollectionUtils.isEmptyMap(newUrlInvokerMap)) {
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" + invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls
                        .toString()));
                return;
			}
			// 获取所有更新后的invoker
            List<Invoker<T>> newInvokers = Collections.unmodifiableList(new ArrayList<>(newUrlInvokerMap.values()));
            // pre-route and build cache, notice that route cache should build on original Invoker list.
            // toMergeMethodInvokerMap() will wrap some invokers having different groups, those wrapped invokers not should be routed.
			// 前置路由处理，并进行缓存构建，证明路由缓存是在原始的invoker列表上进行的
			// toMergeMethodInvokerMap()方法会将属于不同的group的invoker包装起来，这些被包装起来的invoker是不能被路由的
            routerChain.setInvokers(newInvokers);
            this.invokers = multiGroup ? toMergeInvokerList(newInvokers) : newInvokers;
            this.urlInvokerMap = newUrlInvokerMap;

			try {
				// 关闭不使用的invoker
				destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap);
            } catch (Exception e) {
                logger.warn("destroyUnusedInvokers error. ", e);
            }
        }
	}

	/**
	 * 在多group的情形下，合并invoker列表
	 */
	private List<Invoker<T>> toMergeInvokerList(List<Invoker<T>> invokers) {
		List<Invoker<T>> mergedInvokers = new ArrayList<>();
		Map<String, List<Invoker<T>>> groupMap = new HashMap<>();
		// 遍历invokers
		for (Invoker<T> invoker : invokers) {
			// 获取group
			String group = invoker.getUrl().getParameter(GROUP_KEY, "");
			// 为当前group key创建ArrayList
			groupMap.computeIfAbsent(group, k -> new ArrayList<>());
			// 想指定的key添加invoker
			groupMap.get(group).add(invoker);
		}
		// 如果仅存在一个group
		if (groupMap.size() == 1) {
			// 那么就添加这个group中的所有集群
			mergedInvokers.addAll(groupMap.values().iterator().next());
			// 如果有多个group
		} else if (groupMap.size() > 1) {
			// 遍历每个group
			for (List<Invoker<T>> groupList : groupMap.values()) {
				StaticDirectory<T> staticDirectory = new StaticDirectory<>(groupList);
				staticDirectory.buildRouterChain();
				// 利用Cluster将每个集群聚合成一个对外的Cluster，作为外部调用的invoker
				mergedInvokers.add(CLUSTER.join(staticDirectory));
			}
		} else {
			// 如果没有group，那么无需进行合并，直接使用原始的invoker列表
			mergedInvokers = invokers;
		}
		return mergedInvokers;
	}

    /**
     * @param urls
     * @return null : no routers ,do nothing
	 * null: 没有路由器，什么都不做
	 * 其他: 返回路由器规则列表
     */
    private Optional<List<Router>> toRouters(List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return Optional.empty();
        }

        List<Router> routers = new ArrayList<>();
        for (URL url : urls) {
			// 如果协议是"empty://"，代表当前URL不需要进行路由规则更新，游标+1
            if (EMPTY_PROTOCOL.equals(url.getProtocol())) {
                continue;
			}
			// 获取路由类型
            String routerType = url.getParameter(ROUTER_KEY);
			// 设置路由类型
            if (routerType != null && routerType.length() > 0) {
                url = url.setProtocol(routerType);
            }
			try {
				// 通过路由工厂获取URL的路由规则
                Router router = ROUTER_FACTORY.getRouter(url);
				// 避免重复添加
                if (!routers.contains(router)) {
                    routers.add(router);
                }
            } catch (Throwable t) {
                logger.error("convert router url to router error, url: " + url, t);
            }
        }

        return Optional.of(routers);
    }

	/**
	 * 将URL转换为invoker，如果URL已经被引用，则不会进行重新引用
     * @param urls
     * @return invokers
     */
    private Map<String, Invoker<T>> toInvokers(List<URL> urls) {
        Map<String, Invoker<T>> newUrlInvokerMap = new HashMap<>();
        if (urls == null || urls.isEmpty()) {
            return newUrlInvokerMap;
        }
        Set<String> keys = new HashSet<>();
		// 从consumer信息中获取Protocol，如果指定了Protocol，可以获取到，如果未指定，返回null
        String queryProtocols = this.queryMap.get(PROTOCOL_KEY);
        for (URL providerUrl : urls) {
			// 如果protocol配置在引用的一段，则必须在protocol匹配的情况下才会选择
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;
                String[] acceptProtocols = queryProtocols.split(",");
                for (String acceptProtocol : acceptProtocols) {
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }
                if (!accept) {
                    continue;
                }
			}
			// 如果provider的protocol为"empty://"，不进行处理
            if (EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                continue;
			}
			// 如果没有provider提供的协议，不进行处理
            if (!ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() +
                        " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() +
                        " to consumer " + NetUtils.getLocalHost() + ", supported protocol: " +
                        ExtensionLoader.getExtensionLoader(Protocol.class).getSupportedExtensions()));
                continue;
			}
			// 合并URL，主要是将providerUrl的参数和consumerUrl的参数进行合并
            URL url = mergeUrl(providerUrl);
			// 存储在map中，key即为url
			String key = url.toFullString();
			// 重复的url不进行添加
			if (keys.contains(key)) {
                continue;
            }
            keys.add(key);
			// 缓存的key是没有合并consumerUrl参数的url，无论consumer如何合并参数，如果服务端url发生变化，需要进行重新引用
			Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap;
            Invoker<T> invoker = localUrlInvokerMap == null ? null : localUrlInvokerMap.get(key);
			// 没有在缓存中，重新引用
			if (invoker == null) {
                try {
                    boolean enabled = true;
                    if (url.hasParameter(DISABLED_KEY)) {
                        enabled = !url.getParameter(DISABLED_KEY, false);
                    } else {
                        enabled = url.getParameter(ENABLED_KEY, true);
                    }
                    if (enabled) {
						// 创建一个InvokerDelegate代理来进行重新引用
                        invoker = new InvokerDelegate<>(protocol.refer(serviceType, url), url, providerUrl);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }
                if (invoker != null) { // Put new invoker in cache
                    newUrlInvokerMap.put(key, invoker);
                }
            } else {
                newUrlInvokerMap.put(key, invoker);
            }
        }
        keys.clear();
        return newUrlInvokerMap;
    }

    /**
     * Merge url parameters. the order is: override > -D >Consumer > Provider
	 *
	 * @param providerUrl provider的URL
     * @return
     */
    private URL mergeUrl(URL providerUrl) {
		// 将consumer的参数合并到provider的URL中
        providerUrl = ClusterUtils.mergeUrl(providerUrl, queryMap); // Merge the consumer side parameters
		// 合并配置规则
        providerUrl = overrideWithConfigurator(providerUrl);
		// 添加校验参数
		// 如果check=false，则无论连接时成功还是失败的，总是创建invoker
		providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false));

		// 合并provider的参数
		// 仅合并provider参数，directoryUrl和overrideUrl的合并是在notify()方法的最后，这里是不能处理的
		this.overrideDirectoryUrl = this.overrideDirectoryUrl.addParametersIfAbsent(providerUrl.getParameters());

		// 对Dubbo 1.0版本的兼容
        if ((providerUrl.getPath() == null || providerUrl.getPath()
				.length() == 0) && DUBBO_PROTOCOL.equals(providerUrl.getProtocol())) {
            String path = directoryUrl.getParameter(INTERFACE_KEY);
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        return providerUrl;
	}

	/**
	 * 合并配置规则
	 */
	private URL overrideWithConfigurator(URL providerUrl) {
		// 使用配置信息配置URL，适用于dubbo 2.6及以前的版本
		providerUrl = overrideWithConfigurators(this.configurators, providerUrl);

		// 从app-name.configurators中获取配置信息进行配置
		providerUrl = overrideWithConfigurators(CONSUMER_CONFIGURATION_LISTENER.getConfigurators(), providerUrl);

		// 从service-name.configurators中获取配置信息进行配置
		if (serviceConfigurationListener != null) {
			providerUrl = overrideWithConfigurators(serviceConfigurationListener.getConfigurators(), providerUrl);
		}

		return providerUrl;
	}

    private URL overrideWithConfigurators(List<Configurator> configurators, URL url) {
		// 使用配置信息配置URL
        if (CollectionUtils.isNotEmpty(configurators)) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
        }
        return url;
    }

	/**
	 * 关闭所有的invoker
     */
    private void destroyAllInvokers() {
		// 从本地引用中获取invoker集合
        Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
        if (localUrlInvokerMap != null) {
			// 遍历每个URL下的invoker，进行销毁
            for (Invoker<T> invoker : new ArrayList<>(localUrlInvokerMap.values())) {
                try {
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
			}
			// 清除invoker，方便进行内存回收
            localUrlInvokerMap.clear();
        }
        invokers = null;
    }

	/**
	 * 检查在缓存中的invoker是否需要销毁
	 * 如果URL中的refer.autodestroy=false，那么invoker集合只允许增加，不允许减少，可能会存在引用泄露
     * @param oldUrlInvokerMap
     * @param newUrlInvokerMap
     */
    private void destroyUnusedInvokers(Map<String, Invoker<T>> oldUrlInvokerMap, Map<String, Invoker<T>> newUrlInvokerMap) {
        if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
            destroyAllInvokers();
            return;
		}
		// 获取删除的invoker
        List<String> deleted = null;
        if (oldUrlInvokerMap != null) {
            Collection<Invoker<T>> newInvokers = newUrlInvokerMap.values();
            for (Map.Entry<String, Invoker<T>> entry : oldUrlInvokerMap.entrySet()) {
                if (!newInvokers.contains(entry.getValue())) {
                    if (deleted == null) {
                        deleted = new ArrayList<>();
                    }
                    deleted.add(entry.getKey());
                }
            }
		}
		// 从旧invoker缓存的集合中销毁已经删除的invoker
        if (deleted != null) {
            for (String url : deleted) {
                if (url != null) {
                    Invoker<T> invoker = oldUrlInvokerMap.remove(url);
                    if (invoker != null) {
                        try {
                            invoker.destroy();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destroy invoker[" + invoker.getUrl() + "] success. ");
                            }
                        } catch (Exception e) {
                            logger.warn("destroy invoker[" + invoker.getUrl() + "] failed. " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
	}

	/**
	 * 注册中心式获取invoker列表
	 */
	@Override
	public List<Invoker<T>> doList(Invocation invocation) {
		// 已禁止
		// 1. 没有服务provider
		// 2. 服务provider已经被禁用
		if (forbidden) {
			throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "No provider available from registry " +
					getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " +
					NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() +
					", please check status of providers(disabled, not registered or in blacklist).");
		}
		// 如果是多group，返回invokers，invokers此时已经分好group
		if (multiGroup) {
			return this.invokers == null ? Collections.emptyList() : this.invokers;
		}
		// 此时证明是单group
		List<Invoker<T>> invokers = null;
		try {
			// 从缓存中获取invoker，只会过滤运行时invoker
			invokers = routerChain.route(getConsumerUrl(), invocation);
		} catch (Throwable t) {
			logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
		}
		return invokers == null ? Collections.emptyList() : invokers;
	}

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public URL getUrl() {
        return this.overrideDirectoryUrl;
    }

    public URL getRegisteredConsumerUrl() {
        return registeredConsumerUrl;
    }

    public void setRegisteredConsumerUrl(URL registeredConsumerUrl) {
        this.registeredConsumerUrl = registeredConsumerUrl;
    }

    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<String, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        if (localUrlInvokerMap != null && localUrlInvokerMap.size() > 0) {
            for (Invoker<T> invoker : new ArrayList<>(localUrlInvokerMap.values())) {
                if (invoker.isAvailable()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void buildRouterChain(URL url) {
        this.setRouterChain(RouterChain.buildChain(url));
    }

	/**
	 * 用于测试
     */
    public Map<String, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    public List<Invoker<T>> getInvokers() {
        return invokers;
    }

    private boolean isValidCategory(URL url) {
        String category = url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
        if ((ROUTERS_CATEGORY.equals(category) || ROUTE_PROTOCOL.equals(url.getProtocol())) ||
                PROVIDERS_CATEGORY.equals(category) ||
                CONFIGURATORS_CATEGORY.equals(category) || DYNAMIC_CONFIGURATORS_CATEGORY.equals(category) ||
                APP_DYNAMIC_CONFIGURATORS_CATEGORY.equals(category)) {
            return true;
        }
        logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " +
                getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
        return false;
    }

    private boolean isNotCompatibleFor26x(URL url) {
        return StringUtils.isEmpty(url.getParameter(COMPATIBLE_CONFIG_KEY));
	}

	/**
	 * 复写Directory的URL
	 */
	private void overrideDirectoryUrl() {
		// merge override parameters
		this.overrideDirectoryUrl = directoryUrl;
		List<Configurator> localConfigurators = this.configurators; // local reference
		doOverrideUrl(localConfigurators);
		List<Configurator> localAppDynamicConfigurators = CONSUMER_CONFIGURATION_LISTENER.getConfigurators(); // local reference
		doOverrideUrl(localAppDynamicConfigurators);
		if (serviceConfigurationListener != null) {
			List<Configurator> localDynamicConfigurators = serviceConfigurationListener.getConfigurators(); // local reference
			doOverrideUrl(localDynamicConfigurators);
		}
	}

    private void doOverrideUrl(List<Configurator> configurators) {
        if (CollectionUtils.isNotEmpty(configurators)) {
            for (Configurator configurator : configurators) {
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }
    }

	/**
	 * 代理类，用于存储注册中心发送的URL，可以在重新引用时，在providerUrl queryMap overrideMap的基础上进行重新组装
     * @param <T>
     */
    private static class InvokerDelegate<T> extends InvokerWrapper<T> {
        private URL providerUrl;

        public InvokerDelegate(Invoker<T> invoker, URL url, URL providerUrl) {
            super(invoker, url);
            this.providerUrl = providerUrl;
        }

        public URL getProviderUrl() {
            return providerUrl;
        }
    }

    private static class ReferenceConfigurationListener extends AbstractConfiguratorListener {
        private RegistryDirectory directory;
        private URL url;

        ReferenceConfigurationListener(RegistryDirectory directory, URL url) {
            this.directory = directory;
            this.url = url;
            this.initWith(DynamicConfiguration.getRuleKey(url) + CONFIGURATORS_SUFFIX);
        }

        @Override
        protected void notifyOverrides() {
            // to notify configurator/router changes
            directory.refreshInvoker(Collections.emptyList());
        }
    }

    private static class ConsumerConfigurationListener extends AbstractConfiguratorListener {
        List<RegistryDirectory> listeners = new ArrayList<>();

        ConsumerConfigurationListener() {
            this.initWith(ApplicationModel.getApplication() + CONFIGURATORS_SUFFIX);
        }

        void addNotifyListener(RegistryDirectory listener) {
            this.listeners.add(listener);
        }

        @Override
        protected void notifyOverrides() {
            listeners.forEach(listener -> listener.refreshInvoker(Collections.emptyList()));
        }
    }

}
