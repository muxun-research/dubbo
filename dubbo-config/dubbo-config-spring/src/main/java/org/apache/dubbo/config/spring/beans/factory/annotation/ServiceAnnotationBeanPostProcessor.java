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
package org.apache.dubbo.config.spring.beans.factory.annotation;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.annotation.Method;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.spring.ServiceBean;
import org.apache.dubbo.config.spring.context.annotation.DubboClassPathBeanDefinitionScanner;
import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.context.annotation.AnnotationConfigUtils;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.context.annotation.ConfigurationClassPostProcessor;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.config.spring.beans.factory.annotation.ServiceBeanNameBuilder.create;
import static org.apache.dubbo.config.spring.util.AnnotationUtils.resolveServiceInterfaceClass;
import static org.apache.dubbo.config.spring.util.ObjectUtils.of;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;
import static org.springframework.context.annotation.AnnotationConfigUtils.CONFIGURATION_BEAN_NAME_GENERATOR;
import static org.springframework.core.annotation.AnnotatedElementUtils.findMergedAnnotation;
import static org.springframework.core.annotation.AnnotationUtils.getAnnotationAttributes;
import static org.springframework.util.ClassUtils.resolveClassName;

/**
 * {@link Service} Annotation
 * {@link BeanDefinitionRegistryPostProcessor Bean Definition Registry Post Processor}
 *
 * @since 2.5.8
 */
public class ServiceAnnotationBeanPostProcessor implements BeanDefinitionRegistryPostProcessor, EnvironmentAware,
        ResourceLoaderAware, BeanClassLoaderAware {


    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Set<String> packagesToScan;

    private Environment environment;

    private ResourceLoader resourceLoader;

    private ClassLoader classLoader;

    public ServiceAnnotationBeanPostProcessor(String... packagesToScan) {
        this(Arrays.asList(packagesToScan));
    }

    public ServiceAnnotationBeanPostProcessor(Collection<String> packagesToScan) {
        this(new LinkedHashSet<>(packagesToScan));
    }

    public ServiceAnnotationBeanPostProcessor(Set<String> packagesToScan) {
        this.packagesToScan = packagesToScan;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {

        Set<String> resolvedPackagesToScan = resolvePackagesToScan(packagesToScan);

        if (!CollectionUtils.isEmpty(resolvedPackagesToScan)) {
            registerServiceBeans(resolvedPackagesToScan, registry);
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("packagesToScan is empty , ServiceBean registry will be ignored!");
            }
        }

    }


    /**
     * Registers Beans whose classes was annotated {@link Service}
     *
     * @param packagesToScan The base packages to scan
     * @param registry       {@link BeanDefinitionRegistry}
     */
    private void registerServiceBeans(Set<String> packagesToScan, BeanDefinitionRegistry registry) {
		// dubbo自定义的路径扫描器
        DubboClassPathBeanDefinitionScanner scanner =
                new DubboClassPathBeanDefinitionScanner(registry, environment, resourceLoader);
		// 获取BeanNameGenerator
        BeanNameGenerator beanNameGenerator = resolveBeanNameGenerator(registry);
		// 设置扫描器的beanNameGenerator
        scanner.setBeanNameGenerator(beanNameGenerator);
		// 设置扫描器的过滤条件，只扫描@Service
        scanner.addIncludeFilter(new AnnotationTypeFilter(Service.class));

		// 由于之前废弃了一个@Service注解，造成在升级dubbo版本之后，无法兼容以前的版本的，故也扫描之前的版本
		// issue地址：https://github.com/apache/dubbo/issues/4330
        scanner.addIncludeFilter(new AnnotationTypeFilter(com.alibaba.dubbo.config.annotation.Service.class));
		// 遍历需要扫描的包路径
        for (String packageToScan : packagesToScan) {
			// 首先注册@Service
            scanner.scan(packageToScan);

            // Finds all BeanDefinitionHolders of @Service whether @ComponentScan scans or not.
			// 查到所有的
            Set<BeanDefinitionHolder> beanDefinitionHolders =
                    findServiceBeanDefinitionHolders(scanner, packageToScan, registry, beanNameGenerator);

            if (!CollectionUtils.isEmpty(beanDefinitionHolders)) {
				// 如果存在BeanDefinitionHolder，遍历并注册Bean
                for (BeanDefinitionHolder beanDefinitionHolder : beanDefinitionHolders) {
                    registerServiceBean(beanDefinitionHolder, registry, scanner);
                }
				// info日志记录注册信息
                if (logger.isInfoEnabled()) {
                    logger.info(beanDefinitionHolders.size() + " annotated Dubbo's @Service Components { " +
                            beanDefinitionHolders +
                            " } were scanned under package[" + packageToScan + "]");
                }

            } else {
				// 指定包下没有找到可注册的BeanDefinitionHolder，warn日志记录
                if (logger.isWarnEnabled()) {
                    logger.warn("No Spring Bean annotating Dubbo's @Service was found under package["
                            + packageToScan + "]");
                }

            }

        }

    }

    /**
     * It'd better to use BeanNameGenerator instance that should reference
     * {@link ConfigurationClassPostProcessor#componentScanBeanNameGenerator},
     * thus it maybe a potential problem on bean name generation.
     *
     * @param registry {@link BeanDefinitionRegistry}
     * @return {@link BeanNameGenerator} instance
     * @see SingletonBeanRegistry
     * @see AnnotationConfigUtils#CONFIGURATION_BEAN_NAME_GENERATOR
     * @see ConfigurationClassPostProcessor#processConfigBeanDefinitions
     * @since 2.5.8
     */
    private BeanNameGenerator resolveBeanNameGenerator(BeanDefinitionRegistry registry) {

        BeanNameGenerator beanNameGenerator = null;

        if (registry instanceof SingletonBeanRegistry) {
            SingletonBeanRegistry singletonBeanRegistry = SingletonBeanRegistry.class.cast(registry);
            beanNameGenerator = (BeanNameGenerator) singletonBeanRegistry.getSingleton(CONFIGURATION_BEAN_NAME_GENERATOR);
        }

        if (beanNameGenerator == null) {

            if (logger.isInfoEnabled()) {

                logger.info("BeanNameGenerator bean can't be found in BeanFactory with name ["
                        + CONFIGURATION_BEAN_NAME_GENERATOR + "]");
                logger.info("BeanNameGenerator will be a instance of " +
                        AnnotationBeanNameGenerator.class.getName() +
                        " , it maybe a potential problem on bean name generation.");
            }

            beanNameGenerator = new AnnotationBeanNameGenerator();

        }

        return beanNameGenerator;

    }

    /**
	 * 构建一个使用@Service注解的BeanDefinitionHolder集合
     * @param scanner       {@link ClassPathBeanDefinitionScanner}
	 * @param packageToScan 扫描包路径
     * @param registry      {@link BeanDefinitionRegistry}
	 * @return 非空BeanDefinitionHolder集合
     * @since 2.5.8
     */
    private Set<BeanDefinitionHolder> findServiceBeanDefinitionHolders(
            ClassPathBeanDefinitionScanner scanner, String packageToScan, BeanDefinitionRegistry registry,
            BeanNameGenerator beanNameGenerator) {
		// 扫描指定包路径下的类，组装成ScannedGenericBeanDefinition
        Set<BeanDefinition> beanDefinitions = scanner.findCandidateComponents(packageToScan);
		// 创建组装成BeanDefinitionHolder的集合
        Set<BeanDefinitionHolder> beanDefinitionHolders = new LinkedHashSet<>(beanDefinitions.size());

        for (BeanDefinition beanDefinition : beanDefinitions) {
			// 生成beanName，默认情况下是只取类名
            String beanName = beanNameGenerator.generateBeanName(beanDefinition, registry);
			// 组装成BeanDefinitionHolder
            BeanDefinitionHolder beanDefinitionHolder = new BeanDefinitionHolder(beanDefinition, beanName);
            beanDefinitionHolders.add(beanDefinitionHolder);

        }
		// 返回组装好的BeanDefinitionHolder集合
        return beanDefinitionHolders;

    }

    /**
	 * 将包含{@link Service}注解的{@link BeanDefinition}注册为{@link ServiceBean}
     * @param beanDefinitionHolder
     * @param registry
     * @param scanner
     * @see ServiceBean
     * @see BeanDefinition
     */
    private void registerServiceBean(BeanDefinitionHolder beanDefinitionHolder, BeanDefinitionRegistry registry,
                                     DubboClassPathBeanDefinitionScanner scanner) {
		// 使用BeanDefinition加载指定的类
        Class<?> beanClass = resolveClass(beanDefinitionHolder);
		// 获取指定类的@Service注解信息
        Annotation service = findServiceAnnotation(beanClass);

		// 获取@Service注解的所有属性信息
        AnnotationAttributes serviceAnnotationAttributes = getAnnotationAttributes(service, false, false);
		// 获取interfaceClass接口
        Class<?> interfaceClass = resolveServiceInterfaceClass(serviceAnnotationAttributes, beanClass);
		// 获取BeanName
        String annotatedServiceBeanName = beanDefinitionHolder.getBeanName();
		// 构建ServiceBean的BeanDefinition
        AbstractBeanDefinition serviceBeanDefinition =
                buildServiceBeanDefinition(service, serviceAnnotationAttributes, interfaceClass, annotatedServiceBeanName);

		// 构建ServiceBean的BeanName，格式为BeanName:group:version
        String beanName = generateServiceBeanName(serviceAnnotationAttributes, interfaceClass);
		// 检测是否存在重复的候选Bean
        if (scanner.checkCandidate(beanName, serviceBeanDefinition)) { // check duplicated candidate bean
			// 注册由ServiceBean组装的自定义Bean
            registry.registerBeanDefinition(beanName, serviceBeanDefinition);
			// 注册成功，info日志记录
            if (logger.isInfoEnabled()) {
                logger.info("The BeanDefinition[" + serviceBeanDefinition +
                        "] of ServiceBean has been registered with name : " + beanName);
            }

        } else {
			// 存在重复的候选Bean，不进行注册，warn日志记录
            if (logger.isWarnEnabled()) {
                logger.warn("The Duplicated BeanDefinition[" + serviceBeanDefinition +
                        "] of ServiceBean[ bean name : " + beanName +
                        "] was be found , Did @DubboComponentScan scan to same package in many times?");
            }

        }

    }


    /**
	 * 获取@Service注解及其信息
	 * @param beanClass bean的实体类
	 * @return 如果没有找到@Service的注解信息，可能会返回null
     * @since 2.7.3
     */
    private Annotation findServiceAnnotation(Class<?> beanClass) {
		// 获取Service注解的信息，包括Service注解的参数
        Annotation service = findMergedAnnotation(beanClass, Service.class);
        if (service == null) {
			// 由于存在两种Service注解，查找旧版本的Service注解
            service = findMergedAnnotation(beanClass, com.alibaba.dubbo.config.annotation.Service.class);
        }
        return service;
    }

    /**
	 * 构建{@link ServiceBean}的BeanName
     * @param serviceAnnotationAttributes
     * @param interfaceClass              the class of interface annotated {@link Service}
     * @return ServiceBean@interfaceClassName#annotatedServiceBeanName
     * @since 2.7.3
     */
    private String generateServiceBeanName(AnnotationAttributes serviceAnnotationAttributes, Class<?> interfaceClass) {
        ServiceBeanNameBuilder builder = create(interfaceClass, environment)
                .group(serviceAnnotationAttributes.getString("group"))
                .version(serviceAnnotationAttributes.getString("version"));
        return builder.build();
    }

    private Class<?> resolveClass(BeanDefinitionHolder beanDefinitionHolder) {

        BeanDefinition beanDefinition = beanDefinitionHolder.getBeanDefinition();

        return resolveClass(beanDefinition);

    }

    private Class<?> resolveClass(BeanDefinition beanDefinition) {

        String beanClassName = beanDefinition.getBeanClassName();

        return resolveClassName(beanClassName, classLoader);

    }

    private Set<String> resolvePackagesToScan(Set<String> packagesToScan) {
        Set<String> resolvedPackagesToScan = new LinkedHashSet<String>(packagesToScan.size());
        for (String packageToScan : packagesToScan) {
            if (StringUtils.hasText(packageToScan)) {
                String resolvedPackageToScan = environment.resolvePlaceholders(packageToScan.trim());
                resolvedPackagesToScan.add(resolvedPackageToScan);
            }
        }
        return resolvedPackagesToScan;
    }

    /**
     * Build the {@link AbstractBeanDefinition Bean Definition}
     *
     * @param serviceAnnotation
     * @param serviceAnnotationAttributes
     * @param interfaceClass
     * @param annotatedServiceBeanName
     * @return
     * @since 2.7.3
     */
    private AbstractBeanDefinition buildServiceBeanDefinition(Annotation serviceAnnotation,
                                                              AnnotationAttributes serviceAnnotationAttributes,
                                                              Class<?> interfaceClass,
                                                              String annotatedServiceBeanName) {
		// 构建一个ServiceBean类型的BeanDefinitionBuilder
        BeanDefinitionBuilder builder = rootBeanDefinition(ServiceBean.class);
		// 获取BeanDefinition
        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
		// 获取ServiceBean可变的属性集合
        MutablePropertyValues propertyValues = beanDefinition.getPropertyValues();
		// 需要忽略的属性key，这些key会手动一个一个赋值
        String[] ignoreAttributeNames = of("provider", "monitor", "application", "module", "registry", "protocol",
                "interface", "interfaceName", "parameters");
		// 添加属性键值对，这里会删除掉所有使用默认值的属性，即value类型为DefaultValue类型
        propertyValues.addPropertyValues(new AnnotationPropertyValuesAdapter(serviceAnnotation, environment, ignoreAttributeNames));

		// 添加一个ref属性，值为我们自定义Bean的BeanName
        addPropertyReference(builder, "ref", annotatedServiceBeanName);
		// 添加interface属性
        builder.addPropertyValue("interface", interfaceClass.getName());
		// 添加parameters属性
        builder.addPropertyValue("parameters", convertParameters(serviceAnnotationAttributes.getStringArray("parameters")));
		// 以下为手动添加的属性
        List<MethodConfig> methodConfigs = convertMethodConfigs(serviceAnnotationAttributes.get("methods"));
        if (!methodConfigs.isEmpty()) {
            builder.addPropertyValue("methods", methodConfigs);
        }

        /**
		 * {@link org.apache.dubbo.config.ProviderConfig}
         */
        String providerConfigBeanName = serviceAnnotationAttributes.getString("provider");
        if (StringUtils.hasText(providerConfigBeanName)) {
            addPropertyReference(builder, "provider", providerConfigBeanName);
        }

        /**
		 * {@link org.apache.dubbo.config.MonitorConfig}
         */
        String monitorConfigBeanName = serviceAnnotationAttributes.getString("monitor");
        if (StringUtils.hasText(monitorConfigBeanName)) {
            addPropertyReference(builder, "monitor", monitorConfigBeanName);
        }

        /**
		 * {@link org.apache.dubbo.config.ApplicationConfig}
         */
        String applicationConfigBeanName = serviceAnnotationAttributes.getString("application");
        if (StringUtils.hasText(applicationConfigBeanName)) {
            addPropertyReference(builder, "application", applicationConfigBeanName);
        }

        /**
		 * {@link org.apache.dubbo.config.ModuleConfig}
         */
        String moduleConfigBeanName = serviceAnnotationAttributes.getString("module");
        if (StringUtils.hasText(moduleConfigBeanName)) {
            addPropertyReference(builder, "module", moduleConfigBeanName);
        }


        /**
		 * {@link org.apache.dubbo.config.RegistryConfig}
         */
        String[] registryConfigBeanNames = serviceAnnotationAttributes.getStringArray("registry");
		// 处理模板元素
        List<RuntimeBeanReference> registryRuntimeBeanReferences = toRuntimeBeanReferences(registryConfigBeanNames);

        if (!registryRuntimeBeanReferences.isEmpty()) {
            builder.addPropertyValue("registries", registryRuntimeBeanReferences);
        }

        /**
		 * {@link org.apache.dubbo.config.ProtocolConfig}
         */
        String[] protocolConfigBeanNames = serviceAnnotationAttributes.getStringArray("protocol");
		// 处理模板元素
        List<RuntimeBeanReference> protocolRuntimeBeanReferences = toRuntimeBeanReferences(protocolConfigBeanNames);

        if (!protocolRuntimeBeanReferences.isEmpty()) {
            builder.addPropertyValue("protocols", protocolRuntimeBeanReferences);
        }

        return builder.getBeanDefinition();

    }

    private List convertMethodConfigs(Object methodsAnnotation) {
        if (methodsAnnotation == null){
            return Collections.EMPTY_LIST;
        }
        return MethodConfig.constructMethodConfig((Method[])methodsAnnotation);
    }

    private ManagedList<RuntimeBeanReference> toRuntimeBeanReferences(String... beanNames) {

        ManagedList<RuntimeBeanReference> runtimeBeanReferences = new ManagedList<>();

        if (!ObjectUtils.isEmpty(beanNames)) {

            for (String beanName : beanNames) {

                String resolvedBeanName = environment.resolvePlaceholders(beanName);

                runtimeBeanReferences.add(new RuntimeBeanReference(resolvedBeanName));
            }

        }

        return runtimeBeanReferences;

    }

    private void addPropertyReference(BeanDefinitionBuilder builder, String propertyName, String beanName) {
        String resolvedBeanName = environment.resolvePlaceholders(beanName);
        builder.addPropertyReference(propertyName, resolvedBeanName);
    }


    private Map<String, String> convertParameters(String[] parameters) {
        if (ArrayUtils.isEmpty(parameters)) {
            return null;
        }

        if (parameters.length % 2 != 0) {
            throw new IllegalArgumentException("parameter attribute must be paired with key followed by value");
        }

        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < parameters.length; i += 2) {
            map.put(parameters[i], parameters[i + 1]);
        }
        return map;
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

}
