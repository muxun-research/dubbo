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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.profiler.ProfilerSwitch;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvocationProfilerUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcServiceContext;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_LOADBALANCE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_RESELECT_COUNT;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLE_CONNECTIVITY_VALIDATION;
import static org.apache.dubbo.common.constants.CommonConstants.LOADBALANCE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RESELECT_COUNT;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_FAILED_RESELECT_INVOKERS;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_AVAILABLE_CHECK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_STICKY_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_AVAILABLE_CHECK;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_STICKY;

/**
 * AbstractClusterInvoker
 */
public abstract class AbstractClusterInvoker<T> implements ClusterInvoker<T> {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(AbstractClusterInvoker.class);

    protected Directory<T> directory;

    protected boolean availableCheck;

    private volatile int reselectCount = DEFAULT_RESELECT_COUNT;

	private AtomicBoolean destroyed = new AtomicBoolean(false);
	/**
	 * 粘滞连接的invoker
	 * 粘滞连接用于有状态服务，尽可能让客户端总向同一提供者发起调用，除非该提供者挂了，再连另一台
	 * 粘滞连接将自动开启延迟连接，以减少长连接数
	 */
	private volatile Invoker<T> stickyInvoker = null;
    private volatile boolean enableConnectivityValidation = true;

    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    private volatile Invoker<T> stickyInvoker = null;

    public AbstractClusterInvoker() {}

    public AbstractClusterInvoker(Directory<T> directory) {
        this(directory, directory.getUrl());
    }
	public AbstractClusterInvoker(Directory<T> directory) {
		this(directory, directory.getUrl());
	}

	public AbstractClusterInvoker(Directory<T> directory, URL url) {
		if (directory == null) {
			throw new IllegalArgumentException("service directory == null");
		}

		this.directory = directory;
		//sticky: invoker.isAvailable() should always be checked before using when availablecheck is true.
		this.availablecheck = url.getParameter(CLUSTER_AVAILABLE_CHECK_KEY, DEFAULT_CLUSTER_AVAILABLE_CHECK);
	}
        this.directory = directory;
        // sticky: invoker.isAvailable() should always be checked before using when availablecheck is true.
        this.availableCheck = url.getParameter(CLUSTER_AVAILABLE_CHECK_KEY, DEFAULT_CLUSTER_AVAILABLE_CHECK);
        Configuration configuration = ConfigurationUtils.getGlobalConfiguration(url.getOrDefaultModuleModel());
        this.reselectCount = configuration.getInt(RESELECT_COUNT, DEFAULT_RESELECT_COUNT);
        this.enableConnectivityValidation = configuration.getBoolean(ENABLE_CONNECTIVITY_VALIDATION, true);
    }

    @Override
    public Class<T> getInterface() {
        return getDirectory().getInterface();
    }

    @Override
    public URL getUrl() {
        return getDirectory().getConsumerUrl();
    }

    @Override
    public URL getRegistryUrl() {
        return getDirectory().getUrl();
    }

    @Override
    public boolean isAvailable() {
        Invoker<T> invoker = stickyInvoker;
        if (invoker != null) {
            return invoker.isAvailable();
        }
        return getDirectory().isAvailable();
    }

    @Override
    public Directory<T> getDirectory() {
        return directory;
    }

    @Override
    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            getDirectory().destroy();
        }
    }

    @Override
    public boolean isDestroyed() {
        return destroyed.get();
    }

    /**
     * Select a invoker using loadbalance policy.</br>
     * a) Firstly, select an invoker using loadbalance. If this invoker is in previously selected list, or,
     * if this invoker is unavailable, then continue step b (reselect), otherwise return the first selected invoker</br>
     * <p>
     * b) Reselection, the validation rule for reselection: selected > available. This rule guarantees that
     * the selected invoker has the minimum chance to be one in the previously selected list, and also
     * guarantees this invoker is available.
     * 使用负载均衡策略选择一个invoker
     * 1. 首先根据负载均衡策略选择一个invoker，如果invoker处于之前选择过的列表中，或者选择的invoker是不可用的，则重新进行选取，其他情况下则返回第一次选择的invoker
     * 2. 在重新选择过程中的校验规则为：选择>可用
     * 这个规则保证了选择的invoker拥有最少的
     * @param loadbalance load balance policy
     * @param invocation  invocation
     * @param invokers    invoker candidates
     * @param selected    exclude selected invokers or not
     * @return the invoker which will final to do invoke.
     * @throws RpcException exception
     */
    protected Invoker<T> select(
            LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, List<Invoker<T>> selected)
            throws RpcException {

		if (CollectionUtils.isEmpty(invokers)) {
			return null;
		}
		// 获取调用的方法名称
		String methodName = invocation == null ? StringUtils.EMPTY : invocation.getMethodName();
		// 获取粘滞标记
		boolean sticky = invokers.get(0).getUrl()
				.getMethodParameter(methodName, CLUSTER_STICKY_KEY, DEFAULT_CLUSTER_STICKY);

		// 如果粘滞invoker已经不再是此次调用的invoker了，清除invoker标记
		if (stickyInvoker != null && !invokers.contains(stickyInvoker)) {
			stickyInvoker = null;
		}
		// 如果需要粘滞，
		if (sticky && stickyInvoker != null && (selected == null || !selected.contains(stickyInvoker))) {
			// 经过可用性校验后，使用粘滞invoker
			if (availablecheck && stickyInvoker.isAvailable()) {
				return stickyInvoker;
			}
		}
		// 此时已经经历过粘滞校验，接下来就需要根据策略进行选择
		Invoker<T> invoker = doSelect(loadbalance, invocation, invokers, selected);
		// 如果可以粘滞，缓存粘滞invoker
		if (sticky) {
			stickyInvoker = invoker;
		}
		return invoker;
	}

    private Invoker<T> doSelect(
            LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, List<Invoker<T>> selected)
            throws RpcException {
        // 边界判断
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }
        if (invokers.size() == 1) {
            Invoker<T> tInvoker = invokers.get(0);
            checkShouldInvalidateInvoker(tInvoker);
            return tInvoker;
        }
        // 调用AbstractLoadBalance#select()方法，从invoker列表中选择一个invoker
        Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);

        // 如果选出的invoker是已经选中的invoker
        // 或者再需要进行可用性检查的情况下，选出的invoker不可用
        boolean isSelected = selected != null && selected.contains(invoker);
        boolean isUnavailable = availableCheck && !invoker.isAvailable() && getUrl() != null;

        if (isUnavailable) {
            invalidateInvoker(invoker);
        }
        if (isSelected || isUnavailable) {
            try
                // 如果没有选取出合适的invoker，采用nextOne的方式{
                Invoker<T> rInvoker = reselect(loadbalance, invocation, invokers, selected, availableCheck);
                if (rInvoker != null) {
                    invoker = rInvoker;
                } else {
                    // 避免碰撞，按照当前的nextOne的形式选取invoker，同时还避免了数组索引溢出
                    int index = invokers.indexOf(invoker);
                    try {
                        // Avoid collision
                        invoker = invokers.get((index + 1) % invokers.size());
                    } catch (Exception e) {
                        logger.warn(
                                CLUSTER_FAILED_RESELECT_INVOKERS,
                                "select invokers exception",
                                "",
                                e.getMessage() + " may because invokers list dynamic change, ignore.", e);
                    }
                }
        } catch(Throwable t){
            logger.error(
                        CLUSTER_FAILED_RESELECT_INVOKERS,
                        "failed to reselect invokers",
                        "",
                        "cluster reselect fail reason is :" + t.getMessage()
                                + " if can not solve, you can set cluster.availablecheck=false in url",
                        t);
            }
        }

        return invoker;
    }

    /**
     * 重新选取invoker
     * 	 * 首先将使用不在已选invoker列表中的invoker，如果所有的invoker都在已选invoker列表中
     * 	 * 那么只好使用负载均衡策略选取出一个可用的invoker
     * @param loadbalance    load balance policy
     * @param invocation     invocation
     * @param invokers       invoker candidates
     * @param selected       exclude selected invokers or not
     * @param availableCheck check invoker available if true
     * @return the reselect result to do invoke
     * @throws RpcException exception
     */
    private Invoker<T> reselect(
            LoadBalance loadbalance,
            Invocation invocation,
            List<Invoker<T>> invokers,
            List<Invoker<T>> selected,
            boolean availableCheck)
            throws RpcException {

        // Allocating one in advance, this list is certain to be used.
        List<Invoker<T>> reselectInvokers = new ArrayList<>(Math.min(invokers.size(), reselectCount));

        // 1. Try picking some invokers not in `selected`.
        //    1.1. If all selectable invokers' size is smaller than reselectCount, just add all
        //    1.2. If all selectable invokers' size is greater than reselectCount, randomly select reselectCount.
        //            The result size of invokers might smaller than reselectCount due to disAvailable or de-duplication
        // (might be zero).
        //            This means there is probable that reselectInvokers is empty however all invoker list may contain
        // available invokers.
        //            Use reselectCount can reduce retry times if invokers' size is huge, which may lead to long time
        // hang up.
        if (reselectCount >= invokers.size()) {
            // 首先，尝试选取出不在已选invoker列表中的invoker
            for (Invoker<T> invoker : invokers) {
                // check if available
                if (availableCheck && !invoker.isAvailable()) {
                    // add to invalidate invoker
                    invalidateInvoker(invoker);
                    continue;
                }

                if (selected == null || !selected.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        } else {
            for (int i = 0; i < reselectCount; i++) {
                // select one randomly
                Invoker<T> invoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
                // check if available
                if (availableCheck && !invoker.isAvailable()) {
                    // add to invalidate invoker
                    invalidateInvoker(invoker);
                    continue;
                }
                // de-duplication
                if (selected == null || !selected.contains(invoker) || !reselectInvokers.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        }

        // 然后在非已选invoker列表中，使用负载均衡策略选取出一个invoker
        if (!reselectInvokers.isEmpty()) {
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // 3. reselectInvokers is empty. Unable to find at least one available invoker.
        //    Re-check all the selected invokers. If some in the selected list are available, add to reselectInvokers.
        if (selected != null) {
            for (Invoker<T> invoker : selected) {
                if ((invoker.isAvailable()) // available first
                        && !reselectInvokers.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        }

        // 在从这些可用的invoker中，使用负载均衡策略选取出一个invoker
        if (!reselectInvokers.isEmpty()) {
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // 5. No invoker match, return null.
        return null;
    }

    private void checkShouldInvalidateInvoker(Invoker<T> invoker) {
        if (availableCheck && !invoker.isAvailable()) {
            invalidateInvoker(invoker);
        }
    }

    private void invalidateInvoker(Invoker<T> invoker) {
        if (enableConnectivityValidation) {
            if (getDirectory() != null) {
                getDirectory().addInvalidateInvoker(invoker);
            }
        }
    }

    @Override
    public Result invoke(final Invocation invocation) throws RpcException {
        // 校验集群状态
        checkWhetherDestroyed();

        // 将调用过程上下文附加参数放入到invocation中
        //        Map<String, Object> contextAttachments = RpcContext.getClientAttachment().getObjectAttachments();
        //        if (contextAttachments != null && contextAttachments.size() != 0) {
        //            ((RpcInvocation) invocation).addObjectAttachmentsIfAbsent(contextAttachments);
        //        }

        InvocationProfilerUtils.enterDetailProfiler(invocation, () -> "Router route.");
        // 获取invoker列表
        List<Invoker<T>> invokers = list(invocation);
        InvocationProfilerUtils.releaseDetailProfiler(invocation);

        checkInvokers(invokers, invocation);
        // 初始化负载均衡策略
        LoadBalance loadbalance = initLoadBalance(invokers, invocation);
        // 如果是异步调用，设置调用编号
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);

        InvocationProfilerUtils.enterDetailProfiler(
                invocation, () -> "Cluster " + this.getClass().getName() + " invoke.");
        try {
            // 根据同步的执行策略，执行调用
            return doInvoke(invocation, invokers, loadbalance);
        } finally {
            InvocationProfilerUtils.releaseDetailProfiler(invocation);
        }
    }

    protected void checkWhetherDestroyed() {
        if (destroyed.get()) {
            throw new RpcException(
                    "Rpc cluster invoker for " + getInterface() + " on consumer " + NetUtils.getLocalHost()
                            + " use dubbo version " + Version.getVersion()
                            + " is now destroyed! Can not invoke any more.");
        }
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + getUrl().toString();
    }

    protected void checkInvokers(List<Invoker<T>> invokers, Invocation invocation) {
        if (CollectionUtils.isEmpty(invokers)) {
            throw new RpcException(
                    RpcException.NO_INVOKER_AVAILABLE_AFTER_FILTER,
                    "Failed to invoke the method "
                            + RpcUtils.getMethodName(invocation) + " in the service "
                            + getInterface().getName()
                            + ". No provider available for the service "
                            + getDirectory().getConsumerUrl().getServiceKey()
                            + " from registry " + getDirectory()
                            + " on the consumer " + NetUtils.getLocalHost()
                            + " using the dubbo version " + Version.getVersion()
                            + ". Please check if the providers have been started and registered.");
        }
    }

    protected Result invokeWithContext(Invoker<T> invoker, Invocation invocation) {
        Invoker<T> originInvoker = setContext(invoker);
        Result result;
        try {
            if (ProfilerSwitch.isEnableSimpleProfiler()) {
                InvocationProfilerUtils.enterProfiler(
                        invocation,
                        "Invoker invoke. Target Address: " + invoker.getUrl().getAddress());
            }
            setRemote(invoker, invocation);
            result = invoker.invoke(invocation);
        } finally {
            clearContext(originInvoker);
            InvocationProfilerUtils.releaseSimpleProfiler(invocation);
        }
        return result;
    }

    /**
     * Set the remoteAddress and remoteApplicationName so that filter can get them.
     *
     */
    private void setRemote(Invoker<?> invoker, Invocation invocation) {
        invocation.addInvokedInvoker(invoker);
        RpcServiceContext serviceContext = RpcContext.getServiceContext();
        serviceContext.setRemoteAddress(invoker.getUrl().toInetSocketAddress());
        serviceContext.setRemoteApplicationName(invoker.getUrl().getRemoteApplication());
    }

    /**
     * When using a thread pool to fork a child thread, ThreadLocal cannot be passed.
     * In this scenario, please use the invokeWithContextAsync method.
     *
     * @return
     */
    protected Result invokeWithContextAsync(Invoker<T> invoker, Invocation invocation, URL consumerUrl) {
        Invoker<T> originInvoker = setContext(invoker, consumerUrl);
        Result result;
        try {
            result = invoker.invoke(invocation);
        } finally {
            clearContext(originInvoker);
        }
        return result;
    }

    protected abstract Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance)
            throws RpcException;

    protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
        return getDirectory().list(invocation);
    }

    /**
     * 初始化负载均衡策略
     * 如果invoker列表不为空，初始化第一个invoker的负载均衡策略，如果此invoker中没有指定负载均衡粗略，初始化Round-Robin负载均衡策略
     * 如果invoker列表为空，初始化默认的Round-Robin负载均衡策略
     * @param invokers   invokers
     * @param invocation invocation
     * @return LoadBalance instance. if not need init, return null.
     */
    protected LoadBalance initLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {
        ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(invocation.getModuleModel());
        if (CollectionUtils.isNotEmpty(invokers)) {
            return applicationModel
                    .getExtensionLoader(LoadBalance.class)
                    .getExtension(invokers.get(0)
                            .getUrl()
                            .getMethodParameter(
                                    RpcUtils.getMethodName(invocation), LOADBALANCE_KEY, DEFAULT_LOADBALANCE));
        } else {
            return applicationModel.getExtensionLoader(LoadBalance.class).getExtension(DEFAULT_LOADBALANCE);
        }
    }

    private Invoker<T> setContext(Invoker<T> invoker) {
        return setContext(invoker, null);
    }

    private Invoker<T> setContext(Invoker<T> invoker, URL consumerUrl) {
        RpcServiceContext context = RpcContext.getServiceContext();
        Invoker<?> originInvoker = context.getInvoker();
        context.setInvoker(invoker)
                .setConsumerUrl(
                        null != consumerUrl
                                ? consumerUrl
                                : RpcContext.getServiceContext().getConsumerUrl());
        return (Invoker<T>) originInvoker;
    }

    private void clearContext(Invoker<T> invoker) {
        // do nothing
        RpcContext context = RpcContext.getServiceContext();
        context.setInvoker(invoker);
    }
}
