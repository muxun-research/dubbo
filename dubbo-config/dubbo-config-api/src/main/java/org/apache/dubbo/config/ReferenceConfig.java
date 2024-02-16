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
package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.constants.LoggerCodeConstants;
import org.apache.dubbo.common.constants.RegistryConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.cluster.support.registry.ZoneAwareCluster;
import org.apache.dubbo.rpc.model.AsyncMethodInfo;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.DubboStub;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ModuleServiceRepository;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.protocol.injvm.InjvmProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.stub.StubSuppliers;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR_CHAR;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_CLUSTER_DOMAIN;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_MESH_PORT;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.LOCALHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.MESH_ENABLE;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROXY_CLASS_REF;
import static org.apache.dubbo.common.constants.CommonConstants.REVISION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SEMICOLON_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SVC;
import static org.apache.dubbo.common.constants.CommonConstants.TRIPLE;
import static org.apache.dubbo.common.constants.CommonConstants.UNLOAD_CLUSTER_RELATED;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_NO_VALID_PROVIDER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_DESTROY_INVOKER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_LOAD_ENV_VARIABLE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_NO_METHOD_FOUND;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_PROPERTY_CONFLICT;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDED_BY;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;
import static org.apache.dubbo.common.utils.StringUtils.splitToSet;
import static org.apache.dubbo.config.Constants.DUBBO_IP_TO_REGISTRY;
import static org.apache.dubbo.registry.Constants.CONSUMER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTER_IP_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;
import static org.apache.dubbo.rpc.cluster.Constants.PEER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;

/**
 * Please avoid using this class for any new application,
 * use {@link ReferenceConfigBase} instead.
 */
public class ReferenceConfig<T> extends ReferenceConfigBase<T> {

    public static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(ReferenceConfig.class);

    /**
     * The {@link Protocol} implementation with adaptive functionality,it will be different in different scenarios.
     * A particular {@link Protocol} implementation is determined by the protocol attribute in the {@link URL}.
     * For example:
     *
     * <li>when the url is registry://224.5.6.7:1234/org.apache.dubbo.registry.RegistryService?application=dubbo-sample,
     * then the protocol is <b>RegistryProtocol</b></li>
     *
     * <li>when the url is dubbo://224.5.6.7:1234/org.apache.dubbo.config.api.DemoService?application=dubbo-sample, then
     * the protocol is <b>DubboProtocol</b></li>
     * <p>
     * Actually，when the {@link ExtensionLoader} init the {@link Protocol} instants,it will automatically wrap three
     * layers, and eventually will get a <b>ProtocolSerializationWrapper</b> or <b>ProtocolFilterWrapper</b> or <b>ProtocolListenerWrapper</b>
     */
    private Protocol protocolSPI;

    /**
     * A {@link ProxyFactory} implementation that will generate a reference service's proxy,the JavassistProxyFactory is
     * its default implementation
     */
    private ProxyFactory proxyFactory;

    private ConsumerModel consumerModel;

    /**
     * The interface proxy reference
     */
    private transient volatile T ref;

    /**
     * The invoker of the reference service
     */
    private transient volatile Invoker<?> invoker;

    /**
     * The flag whether the ReferenceConfig has been initialized
     */
    private transient volatile boolean initialized;

    /**
     * whether this ReferenceConfig has been destroyed
     */
    private transient volatile boolean destroyed;

    /**
     * The service names that the Dubbo interface subscribed.
     *
     * @since 2.7.8
     */
    private String services;

    public ReferenceConfig() {
        super();
    }

    public ReferenceConfig(ModuleModel moduleModel) {
        super(moduleModel);
    }

    public ReferenceConfig(Reference reference) {
        super(reference);
    }

    public ReferenceConfig(ModuleModel moduleModel, Reference reference) {
        super(moduleModel, reference);
    }

    @Override
    protected void postProcessAfterScopeModelChanged(ScopeModel oldScopeModel, ScopeModel newScopeModel) {
        super.postProcessAfterScopeModelChanged(oldScopeModel, newScopeModel);

        protocolSPI = this.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        proxyFactory = this.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    }

    /**
     * Get a string presenting the service names that the Dubbo interface subscribed.
     * If it is a multiple-values, the content will be a comma-delimited String.
     *
     * @return non-null
     * @see RegistryConstants#SUBSCRIBED_SERVICE_NAMES_KEY
     * @since 2.7.8
     */
    @Deprecated
    @Parameter(key = SUBSCRIBED_SERVICE_NAMES_KEY)
    public String getServices() {
        return services;
    }

    /**
     * It's an alias method for {@link #getServices()}, but the more convenient.
     *
     * @return the String {@link List} presenting the Dubbo interface subscribed
     * @since 2.7.8
     */
    @Deprecated
    @Parameter(excluded = true)
    public Set<String> getSubscribedServices() {
        return splitToSet(getServices(), COMMA_SEPARATOR_CHAR);
    }

    /**
     * Set the service names that the Dubbo interface subscribed.
     *
     * @param services If it is a multiple-values, the content will be a comma-delimited String.
     * @since 2.7.8
     */
    public void setServices(String services) {
        this.services = services;
    }
    public synchronized T get() {

        checkAndUpdateSubConfigs();

    @Override
    @Transient
    public T get(boolean check) {
        if (destroyed) {
            throw new IllegalStateException("The invoker of ReferenceConfig(" + url + ") has already destroyed!");
        }

		}
		// 如果接口代理引用没有实现，进行实现
        if (ref == null) {
            if (getScopeModel().isLifeCycleManagedExternally()) {
                // prepare model for reference
                getScopeModel().getDeployer().prepare();
            } else {
                // ensure start module, compatible with old api usage
                getScopeModel().getDeployer().start();
            }

            init(check);
        }

        return ref;
    }

    @Override
    public void checkOrDestroy(long timeout) {
        if (!initialized || ref == null) {
            return;
        }
        try {
            checkInvokerAvailable(timeout);
        } catch (Throwable t) {
            logAndCleanup(t);
            throw t;
        }
    }

    private void logAndCleanup(Throwable t) {
        try {
            if (invoker != null) {
                invoker.destroy();
            }
        } catch (Throwable destroy) {
            logger.warn(
                    CONFIG_FAILED_DESTROY_INVOKER,
                    "",
                    "",
                    "Unexpected error occurred when destroy invoker of ReferenceConfig(" + url + ").",
                    t);
        }
        if (consumerModel != null) {
            ModuleServiceRepository repository = getScopeModel().getServiceRepository();
            repository.unregisterConsumer(consumerModel);
        }
        initialized = false;
        invoker = null;
        ref = null;
        consumerModel = null;
        serviceMetadata.setTarget(null);
        serviceMetadata.getAttributeMap().remove(PROXY_CLASS_REF);

        // Thrown by checkInvokerAvailable().
        if (t.getClass() == IllegalStateException.class
                && t.getMessage().contains("No provider available for the service")) {

            // 2-2 - No provider available.
            logger.error(CLUSTER_NO_VALID_PROVIDER, "server crashed", "", "No provider available.", t);
        }
    }

    @Override
    public synchronized void destroy() {
        super.destroy();
        if (destroyed) {
            return;
        }
        destroyed = true;
        try {
            if (invoker != null) {
                invoker.destroy();
            }
        } catch (Throwable t) {
            logger.warn(
                    CONFIG_FAILED_DESTROY_INVOKER,
                    "",
                    "",
                    "Unexpected error occurred when destroy invoker of ReferenceConfig(" + url + ").",
                    t);
        }
        invoker = null;
        ref = null;
        if (consumerModel != null) {
            ModuleServiceRepository repository = getScopeModel().getServiceRepository();
            repository.unregisterConsumer(consumerModel);
        }
    }

    protected synchronized void init() {
        init(true);
    }

    protected synchronized void init(boolean check) {
        if (initialized && ref != null) {
    private void init() {
		// 如果已经初始化，不继续进行初始化
        if (initialized) {
            return;
        }
        try {
            if (!this.isRefreshed()) {
                this.refresh();
            }
            // auto detect proxy type
            String proxyType = getProxy();
            if (StringUtils.isBlank(proxyType) && DubboStub.class.isAssignableFrom(interfaceClass)) {
                setProxy(CommonConstants.NATIVE_STUB);
            }

            // init serviceMetadata
            initServiceMetadata(consumer);

            serviceMetadata.setServiceType(getServiceInterfaceClass());
            // TODO, uncomment this line once service key is unified
            serviceMetadata.generateServiceKey();

            Map<String, String> referenceParameters = appendConfig();

            ModuleServiceRepository repository = getScopeModel().getServiceRepository();
            ServiceDescriptor serviceDescriptor;
            if (CommonConstants.NATIVE_STUB.equals(getProxy())) {
                serviceDescriptor = StubSuppliers.getServiceDescriptor(interfaceName);
                repository.registerService(serviceDescriptor);
            } else {
                serviceDescriptor = repository.registerService(interfaceClass);
            }
            consumerModel = new ConsumerModel(
                    serviceMetadata.getServiceKey(),
                    proxy,
                    serviceDescriptor,
                    getScopeModel(),
                    serviceMetadata,
                    createAsyncMethodInfo(),
                    interfaceClassLoader);

            // Compatible with dependencies on ServiceModel#getReferenceConfig() , and will be removed in a future
            // version.
            consumerModel.setConfig(this);

            repository.registerConsumer(consumerModel);

            serviceMetadata.getAttachments().putAll(referenceParameters);

            ref = createProxy(referenceParameters);

            serviceMetadata.setTarget(ref);
            serviceMetadata.addAttribute(PROXY_CLASS_REF, ref);

            consumerModel.setDestroyRunner(getDestroyRunner());
            consumerModel.setProxyObject(ref);
            consumerModel.initMethodModels();

            if (check) {
                checkInvokerAvailable(0);
            }
        } catch (Throwable t) {
            logAndCleanup(t);

            throw t;
        }
        initialized = true;
    }

    /**
     * convert and aggregate async method info
     *
     * @return Map<String, AsyncMethodInfo>
     */
    private Map<String, AsyncMethodInfo> createAsyncMethodInfo() {
        Map<String, AsyncMethodInfo> attributes = null;
        if (CollectionUtils.isNotEmpty(getMethods())) {
            attributes = new HashMap<>(16);
            for (MethodConfig methodConfig : getMethods()) {
                AsyncMethodInfo asyncMethodInfo = methodConfig.convertMethodConfig2AsyncInfo();
                if (asyncMethodInfo != null) {
                    attributes.put(methodConfig.getName(), asyncMethodInfo);
                }
            }
        }

        return attributes;
    }

    /**
     * Append all configuration required for service reference.
     *
     * @return reference parameters
     */
    private Map<String, String> appendConfig() {
        Map<String, String> map = new HashMap<>(16);

        map.put(INTERFACE_KEY, interfaceName);
        checkStubAndLocal(interfaceClass);
        checkMock(interfaceClass);
        Map<String, String> map = new HashMap<String, String>();
		// 标识是消费者
        map.put(SIDE_KEY, CONSUMER_SIDE);

        ReferenceConfigBase.appendRuntimeParameters(map);

        if (!ProtocolUtils.isGeneric(generic)) {
		// 标识dubbo版本，dubbo包版本，PID，时间戳
        appendRuntimeParameters(map);
        if (!ProtocolUtils.isGeneric(getGeneric())) {
			// 获取接口的version
            String revision = Version.getVersion(interfaceClass, version);
            if (StringUtils.isNotEmpty(revision)) {
			// 存在version的情况，则写入revision
            if (revision != null && revision.length() > 0) {
                map.put(REVISION_KEY, revision);
            }

            String[] methods = methods(interfaceClass);
			}
			// 获取调用接口的所有方法名称
            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
            if (methods.length == 0) {
                logger.warn(
                        CONFIG_NO_METHOD_FOUND,
                        "",
                        "",
                        "No method found in service interface: " + interfaceClass.getName());
                map.put(METHODS_KEY, ANY_VALUE);
            } else {
                map.put(METHODS_KEY, StringUtils.join(new TreeSet<>(Arrays.asList(methods)), COMMA_SEPARATOR));
			} else {
				// 添加methods属性，方法名以","分隔
                map.put(METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(methods)), COMMA_SEPARATOR));
            }
        }

        AbstractConfig.appendParameters(map, getApplication());
        AbstractConfig.appendParameters(map, getModule());
        AbstractConfig.appendParameters(map, consumer);
        AbstractConfig.appendParameters(map, this);

        String hostToRegistry = ConfigUtils.getSystemProperty(DUBBO_IP_TO_REGISTRY);
        if (StringUtils.isEmpty(hostToRegistry)) {
            hostToRegistry = NetUtils.getLocalHost();
        } else if (isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY
                    + ", value:" + hostToRegistry);
        }

        map.put(REGISTER_IP_KEY, hostToRegistry);

        if (CollectionUtils.isNotEmpty(getMethods())) {
            for (MethodConfig methodConfig : getMethods()) {
                AbstractConfig.appendParameters(map, methodConfig, methodConfig.getName());
		}
		// 添加interface属性
        map.put(INTERFACE_KEY, interfaceName);
        appendParameters(map, metrics);
        appendParameters(map, application);
        appendParameters(map, module);
        // remove 'default.' prefix for configs from ConsumerConfig
        // appendParameters(map, consumer, Constants.DEFAULT_KEY);
        appendParameters(map, consumer);
        appendParameters(map, this);
        Map<String, Object> attributes = null;
        if (CollectionUtils.isNotEmpty(methods)) {
            attributes = new HashMap<String, Object>();
            for (MethodConfig methodConfig : methods) {
                appendParameters(map, methodConfig, methodConfig.getName());
                String retryKey = methodConfig.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        map.put(methodConfig.getName() + ".retries", "0");
                    }
                }
            }
        }

        return map;
    }

    @SuppressWarnings({"unchecked"})
    private T createProxy(Map<String, String> referenceParameters) {
        urls.clear();

        meshModeHandleUrl(referenceParameters);

        if (StringUtils.isNotEmpty(url)) {
            // user specified URL, could be peer-to-peer address, or register center's address.
            parseUrl(referenceParameters);
        } else {
            // if protocols not in jvm checkRegistry
            aggregateUrlFromRegistry(referenceParameters);
        }
        createInvoker();
        String hostToRegistry = ConfigUtils.getSystemProperty(DUBBO_IP_TO_REGISTRY);
        if (StringUtils.isEmpty(hostToRegistry)) {
			// 需要进行注册的IP，默认是本机的IP地址
            hostToRegistry = NetUtils.getLocalHost();
        } else if (isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        }
        map.put(REGISTER_IP_KEY, hostToRegistry);
		// map里面存储的是消费者的信息
		ref = createProxy(map);
		// 构建请求服务key，格式group/interfaceName:version
        String serviceKey = URL.buildKey(interfaceName, group, version);
		// 构建消费者模型
        ApplicationModel.initConsumerModel(serviceKey, buildConsumerModel(serviceKey, attributes));
        initialized = true;
    }

        if (logger.isInfoEnabled()) {
            logger.info("Referred dubbo service: [" + referenceParameters.get(INTERFACE_KEY) + "]."
                    + (ProtocolUtils.isGeneric(referenceParameters.get(GENERIC_KEY))
                            ? " it's GenericService reference"
                            : " it's not GenericService reference"));
        }

        URL consumerUrl = new ServiceConfigURL(
                CONSUMER_PROTOCOL,
                referenceParameters.get(REGISTER_IP_KEY),
                0,
                referenceParameters.get(INTERFACE_KEY),
                referenceParameters);
        consumerUrl = consumerUrl.setScopeModel(getScopeModel());
        consumerUrl = consumerUrl.setServiceModel(consumerModel);
        MetadataUtils.publishServiceDefinition(consumerUrl, consumerModel.getServiceModel(), getApplicationModel());

        // create service proxy
        return (T) proxyFactory.getProxy(invoker, ProtocolUtils.isGeneric(generic));
		// 注册Service调用接口，及它的所有方法，ReferenceBean和属性
        return new ConsumerModel(serviceKey, serviceInterface, ref, methods, attributes);
    }

    /**
     * if enable mesh mode, handle url.
     *
     * @param referenceParameters referenceParameters
     */
    private void meshModeHandleUrl(Map<String, String> referenceParameters) {
        if (!checkMeshConfig(referenceParameters)) {
            return;
        }
        if (StringUtils.isNotEmpty(url)) {
            // user specified URL, could be peer-to-peer address, or register center's address.
    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
	// 构建请求注册中心的代理
    private T createProxy(Map<String, String> map) {
        if (shouldJvmRefer(map)) {
            URL url = new URL(LOCAL_PROTOCOL, LOCALHOST_VALUE, 0, interfaceClass.getName()).addParameters(map);
            invoker = REF_PROTOCOL.refer(interfaceClass, url);
            if (logger.isInfoEnabled()) {
                logger.info("The url already exists, mesh no longer processes url: " + url);
            }
            return;
        }

        // get provider namespace if (@DubboReference, <reference provider-namespace="xx"/>) present
        String podNamespace = referenceParameters.get(RegistryConstants.PROVIDER_NAMESPACE);

        // get pod namespace from env if annotation not present the provider namespace
        if (StringUtils.isEmpty(podNamespace)) {
            if (StringUtils.isEmpty(System.getenv("POD_NAMESPACE"))) {
                if (logger.isWarnEnabled()) {
                    logger.warn(
                            CONFIG_FAILED_LOAD_ENV_VARIABLE,
                            "",
                            "",
                            "Can not get env variable: POD_NAMESPACE, it may not be running in the K8S environment , "
                                    + "finally use 'default' replace.");
                }
                podNamespace = "default";
            } else {
                podNamespace = System.getenv("POD_NAMESPACE");
            }
        }

        // In mesh mode, providedBy equals K8S Service name.
        String providedBy = referenceParameters.get(PROVIDED_BY);
        // cluster_domain default is 'cluster.local',generally unchanged.
        String clusterDomain =
                Optional.ofNullable(System.getenv("CLUSTER_DOMAIN")).orElse(DEFAULT_CLUSTER_DOMAIN);
        // By VirtualService and DestinationRule, envoy will generate a new route rule,such as
        // 'demo.default.svc.cluster.local:80',the default port is 80.
        Integer meshPort = Optional.ofNullable(getProviderPort()).orElse(DEFAULT_MESH_PORT);
        // DubboReference default is -1, process it.
        meshPort = meshPort > -1 ? meshPort : DEFAULT_MESH_PORT;
        // get mesh url.
        url = TRIPLE + "://" + providedBy + "." + podNamespace + SVC + clusterDomain + ":" + meshPort;
    }

    /**
     * check if mesh config is correct
     *
     * @param referenceParameters referenceParameters
     * @return mesh config is correct
     */
    private boolean checkMeshConfig(Map<String, String> referenceParameters) {
        if (!"true".equals(referenceParameters.getOrDefault(MESH_ENABLE, "false"))) {
            // In mesh mode, unloadClusterRelated can only be false.
            referenceParameters.put(UNLOAD_CLUSTER_RELATED, "false");
            return false;
        }

        getScopeModel()
                .getConfigManager()
                .getProtocol(TRIPLE)
                .orElseThrow(() -> new IllegalStateException("In mesh mode, a triple protocol must be specified"));

        String providedBy = referenceParameters.get(PROVIDED_BY);
        if (StringUtils.isEmpty(providedBy)) {
            throw new IllegalStateException("In mesh mode, the providedBy of ReferenceConfig is must be set");
        }

        return true;
    }

    /**
     * Parse the directly configured url.
     */
    private void parseUrl(Map<String, String> referenceParameters) {
        String[] us = SEMICOLON_SPLIT_PATTERN.split(url);
        if (ArrayUtils.isNotEmpty(us)) {
            for (String u : us) {
                URL url = URL.valueOf(u);
                if (StringUtils.isEmpty(url.getPath())) {
                    url = url.setPath(interfaceName);
                }
                url = url.setScopeModel(getScopeModel());
                url = url.setServiceModel(consumerModel);
                if (UrlUtils.isRegistry(url)) {
                    urls.add(url.putAttribute(REFER_KEY, referenceParameters));
                } else {
                    URL peerUrl = getScopeModel()
                            .getApplicationModel()
                            .getBeanFactory()
                            .getBean(ClusterUtils.class)
                            .mergeUrl(url, referenceParameters);
                    peerUrl = peerUrl.putAttribute(PEER_KEY, true);
                    urls.add(peerUrl);
                }
            }
        }
    }

    /**
     * Get URLs from the registry and aggregate them.
     */
    private void aggregateUrlFromRegistry(Map<String, String> referenceParameters) {
        checkRegistry();
        List<URL> us = ConfigValidationUtils.loadRegistries(this, false);
        if (CollectionUtils.isNotEmpty(us)) {
            for (URL u : us) {
                URL monitorUrl = ConfigValidationUtils.loadMonitor(this, u);
                if (monitorUrl != null) {
                    u = u.putAttribute(MONITOR_KEY, monitorUrl);
                }
                u = u.setScopeModel(getScopeModel());
                u = u.setServiceModel(consumerModel);
                if (isInjvm() != null && isInjvm()) {
                    u = u.addParameter(LOCAL_PROTOCOL, true);
                }
                urls.add(u.putAttribute(REFER_KEY, referenceParameters));
            }
        }
        if (urls.isEmpty() && shouldJvmRefer(referenceParameters)) {
            URL injvmUrl = new URL(LOCAL_PROTOCOL, LOCALHOST_VALUE, 0, interfaceClass.getName())
                    .addParameters(referenceParameters);
            injvmUrl = injvmUrl.setScopeModel(getScopeModel());
            injvmUrl = injvmUrl.setServiceModel(consumerModel);
            urls.add(injvmUrl.putAttribute(REFER_KEY, referenceParameters));
        }
        if (urls.isEmpty()) {
            throw new IllegalStateException("No such any registry to reference " + interfaceName + " on the consumer "
                    + NetUtils.getLocalHost() + " use dubbo version "
                    + Version.getVersion()
                    + ", please config <dubbo:registry address=\"...\" /> to your spring config.");
        }
    }

    /**
     * \create a reference invoker
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createInvoker() {
        if (urls.size() == 1) {
            URL curUrl = urls.get(0);
            invoker = protocolSPI.refer(interfaceClass, curUrl);
            // registry url, mesh-enable and unloadClusterRelated is true, not need Cluster.
            if (!UrlUtils.isRegistry(curUrl) && !curUrl.getParameter(UNLOAD_CLUSTER_RELATED, false)) {
                List<Invoker<?>> invokers = new ArrayList<>();
                invokers.add(invoker);
                invoker = Cluster.getCluster(getScopeModel(), Cluster.DEFAULT)
                        .join(new StaticDirectory(curUrl, invokers), true);
            }
        } else {
            List<Invoker<?>> invokers = new ArrayList<>();
            URL registryUrl = null;
            for (URL url : urls) {
                // For multi-registry scenarios, it is not checked whether each referInvoker is available.
                // Because this invoker may become available later.
                invokers.add(protocolSPI.refer(interfaceClass, url));

                if (UrlUtils.isRegistry(url)) {
                    // use last registry url
                    registryUrl = url;
                }
            }

            if (registryUrl != null) {
                // registry url is available
                // for multi-subscription scenario, use 'zone-aware' policy by default
                String cluster = registryUrl.getParameter(CLUSTER_KEY, ZoneAwareCluster.NAME);
                // The invoker wrap sequence would be: ZoneAwareClusterInvoker(StaticDirectory) ->
                // FailoverClusterInvoker
                // (RegistryDirectory, routing happens here) -> Invoker
                invoker = Cluster.getCluster(registryUrl.getScopeModel(), cluster, false)
                        .join(new StaticDirectory(registryUrl, invokers), false);
            } else {
                // not a registry url, must be direct invoke.
                if (CollectionUtils.isEmpty(invokers)) {
                    throw new IllegalArgumentException("invokers == null");
        } else {
			// 避免OOM
			urls.clear();
			// 如果我们提供了直接连接地址，可以是provider地址，也可以是注册中心地址
			if (url != null && url.length() > 0) {
                String[] us = SEMICOLON_SPLIT_PATTERN.split(url);
                if (us != null && us.length > 0) {
                    for (String u : us) {
                        URL url = URL.valueOf(u);
                        if (StringUtils.isEmpty(url.getPath())) {
                            url = url.setPath(interfaceName);
                        }
						// 带有registry即为需要连接注册中心
                        if (REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                            urls.add(url.addParameterAndEncoded(REFER_KEY, StringUtils.toQueryString(map)));
                        } else {
                            urls.add(ClusterUtils.mergeUrl(url, map));
                        }
                    }
                }
			} else {
				// 在不是本机直连的情况下，需要连接注册中心
                if (!LOCAL_PROTOCOL.equalsIgnoreCase(getProtocol())){
                    checkRegistry();
					// 获取注册中心的url集合
                    List<URL> us = loadRegistries(false);
                    if (CollectionUtils.isNotEmpty(us)) {
                        for (URL u : us) {
							// 加载监控中心url
                            URL monitorUrl = loadMonitor(u);
                            if (monitorUrl != null) {
                                map.put(MONITOR_KEY, URL.encode(monitorUrl.toFullString()));
                            }
							// 将请求url带上服务引用参数
                            urls.add(u.addParameterAndEncoded(REFER_KEY, StringUtils.toQueryString(map)));
                        }
                    }
					// 不允许出现有引用调用，但是没有提供指定地址，也没有提供注册中心的情况出现
                    if (urls.isEmpty()) {
                        throw new IllegalStateException("No such any registry to reference " + interfaceName + " on the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() + ", please config <dubbo:registry address=\"...\" /> to your spring config.");
                    }
                }
            }
			// 如果仅有一个url，引用服务，返回invoker对象
            if (urls.size() == 1) {
                invoker = REF_PROTOCOL.refer(interfaceClass, urls.get(0));
            } else {
				// 如果存在多个url的情况，
                List<Invoker<?>> invokers = new ArrayList<Invoker<?>>();
                URL registryURL = null;
                for (URL url : urls) {
                    invokers.add(REF_PROTOCOL.refer(interfaceClass, url));
					// 使用最后一个注册中心url
                    if (REGISTRY_PROTOCOL.equals(url.getProtocol())) {
						registryURL = url;
                    }
                }
				// 如果存在可用的注册中心url
				if (registryURL != null) {
					// 对有注册中心的Cluster使用RegistryAwareCluster
                    URL u = registryURL.addParameter(CLUSTER_KEY, RegistryAwareCluster.NAME);
					// invoker的关心为：RegistryAwareClusterInvoker(StaticDirectory) -> FailoverClusterInvoker(RegistryDirectory, will execute route) -> Invoker
                    invoker = CLUSTER.join(new StaticDirectory(u, invokers));
					// 如果没有注册中心url，需要直连，获取invokers中的第一个作为url
				} else {
                    invoker = CLUSTER.join(new StaticDirectory(invokers));
                }
                URL curUrl = invokers.get(0).getUrl();
                String cluster = curUrl.getParameter(CLUSTER_KEY, Cluster.DEFAULT);
                invoker =
                        Cluster.getCluster(getScopeModel(), cluster).join(new StaticDirectory(curUrl, invokers), true);
            }
        }
    }

    private void checkInvokerAvailable(long timeout) throws IllegalStateException {
        if (!shouldCheck()) {
            return;
        }
        boolean available = invoker.isAvailable();
        if (available) {
            return;
		// 启动前检查
        if (shouldCheck() && !invoker.isAvailable()) {
            throw new IllegalStateException("Failed to check the status of the service " + interfaceName + ". No provider available for the service " + (group == null ? "" : group + "/") + interfaceName + (version == null ? "" : ":" + version) + " from the url " + invoker.getUrl() + " to the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());
        }

        long startTime = System.currentTimeMillis();
        long checkDeadline = startTime + timeout;
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            available = invoker.isAvailable();
        } while (!available && checkDeadline > System.currentTimeMillis());
        logger.warn(
                LoggerCodeConstants.REGISTRY_EMPTY_ADDRESS,
                "",
                "",
                "Check reference of [" + getUniqueServiceName() + "] failed very beginning. " + "After "
                        + (System.currentTimeMillis() - startTime) + "ms reties, finally "
                        + (available ? "succeed" : "failed")
                        + ".");
        if (!available) {
            // 2-2 - No provider available.

            IllegalStateException illegalStateException =
                    new IllegalStateException("Failed to check the status of the service "
                            + interfaceName
                            + ". No provider available for the service "
                            + (group == null ? "" : group + "/")
                            + interfaceName + (version == null ? "" : ":" + version)
                            + " from the url "
                            + invoker.getUrl()
                            + " to the consumer "
                            + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());

            logger.error(
                    CLUSTER_NO_VALID_PROVIDER,
                    "provider not started",
                    "",
                    "No provider available.",
                    illegalStateException);

            throw illegalStateException;
        }
    }

    /**
     * 这个方法需要在类创建之后，在其他配置模块使用之前被调用
     * 检查每个配置模块是否适当的创建，并且在必要时可以重写他们的属性
     */
    protected void checkAndUpdateSubConfigs() {
        if (StringUtils.isEmpty(interfaceName)) {
            throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
        }

        // get consumer's global configuration
        completeCompoundConfigs();

        // init some null configuration.
        List<ConfigInitializer> configInitializers = this.getExtensionLoader(ConfigInitializer.class)
                .getActivateExtension(URL.valueOf("configInitializer://"), (String[]) null);
        configInitializers.forEach(e -> e.initReferConfig(this));

        if (getGeneric() == null && getConsumer() != null) {
            setGeneric(getConsumer().getGeneric());
        }
        if (ProtocolUtils.isGeneric(generic)) {
            if (interfaceClass != null && !interfaceClass.equals(GenericService.class)) {
                logger.warn(
                        CONFIG_PROPERTY_CONFLICT,
                        "",
                        "",
                        String.format(
                                "Found conflicting attributes for interface type: [interfaceClass=%s] and [generic=%s], "
                                        + "because the 'generic' attribute has higher priority than 'interfaceClass', so change 'interfaceClass' to '%s'. "
                                        + "Note: it will make this reference bean as a candidate bean of type '%s' instead of '%s' when resolving dependency in Spring.",
                                interfaceClass.getName(),
                                generic,
                                GenericService.class.getName(),
                                GenericService.class.getName(),
                                interfaceClass.getName()));
            }
            interfaceClass = GenericService.class;
        } else {
            try {
                if (getInterfaceClassLoader() != null
                        && (interfaceClass == null || interfaceClass.getClassLoader() != getInterfaceClassLoader())) {
                    interfaceClass = Class.forName(interfaceName, true, getInterfaceClassLoader());
                } else if (interfaceClass == null) {
                    interfaceClass = Class.forName(
                            interfaceName, true, Thread.currentThread().getContextClassLoader());
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        checkStubAndLocal(interfaceClass);
        ConfigValidationUtils.checkMock(interfaceClass, this);

        if (StringUtils.isEmpty(url)) {
            checkRegistry();
        }

        resolveFile();
        ConfigValidationUtils.validateReferenceConfig(this);
        postProcessConfig();
    }

    @Override
    protected void postProcessRefresh() {
        super.postProcessRefresh();
        checkAndUpdateSubConfigs();
    }

    protected void completeCompoundConfigs() {
        super.completeCompoundConfigs(consumer);
        if (consumer != null) {
            if (StringUtils.isEmpty(registryIds)) {
                setRegistryIds(consumer.getRegistryIds());
            }
        }
		// 创建服务代理
        return (T) PROXY_FACTORY.getProxy(invoker);
    }

    /**
     * Figure out should refer the service in the same JVM from configurations. The default behavior is true
     * 1. if injvm is specified, then use it
     * 2. then if a url is specified, then assume it's a remote call
     * 3. otherwise, check scope parameter
     * 4. if scope is not specified but the target service is provided in the same JVM, then prefer to make the local
     * call, which is the default behavior
     */
    protected boolean shouldJvmRefer(Map<String, String> map) {
        boolean isJvmRefer;
        if (isInjvm() == null) {
            // if an url is specified, don't do local reference
            if (StringUtils.isNotEmpty(url)) {
			// 如果一寄给你指定url了，则不使用本地Reference
            if (url != null && url.length() > 0) {
                isJvmRefer = false;
            } else {
                // by default, reference local service if there is
                URL tmpUrl = new ServiceConfigURL("temp", "localhost", 0, map);
                isJvmRefer = InjvmProtocol.getInjvmProtocol(getScopeModel()).isInjvmRefer(tmpUrl);
				// 是否指向本地Reference
                isJvmRefer = InjvmProtocol.getInjvmProtocol().isInjvmRefer(tmpUrl);
            }
        } else {
            isJvmRefer = isInjvm();
        }
        return isJvmRefer;
    }

    private void postProcessConfig() {
        List<ConfigPostProcessor> configPostProcessors = this.getExtensionLoader(ConfigPostProcessor.class)
                .getActivateExtension(URL.valueOf("configPostProcessor://"), (String[]) null);
        configPostProcessors.forEach(component -> component.postProcessReferConfig(this));
    }

    /**
     * Return if ReferenceConfig has been initialized
     * Note: Cannot use `isInitilized` as it may be treated as a Java Bean property
     *
     * @return initialized
     */
    @Transient
    public boolean configInitialized() {
        return initialized;
    }

    /**
     * just for test
     *
     * @return
     */
    @Deprecated
    @Transient
    public Invoker<?> getInvoker() {
        return invoker;
    }

    @Transient
    public Runnable getDestroyRunner() {
        return this::destroy;
    }
}
