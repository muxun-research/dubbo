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
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_AVAILABLE_CHECK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_STICKY_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_AVAILABLE_CHECK;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_STICKY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_LOADBALANCE;
import static org.apache.dubbo.rpc.cluster.Constants.LOADBALANCE_KEY;

/**
 * AbstractClusterInvoker
 */
public abstract class AbstractClusterInvoker<T> implements Invoker<T> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractClusterInvoker.class);

	protected final Directory<T> directory;

	protected final boolean availablecheck;

	private AtomicBoolean destroyed = new AtomicBoolean(false);
	/**
	 * 粘滞连接的invoker
	 * 粘滞连接用于有状态服务，尽可能让客户端总向同一提供者发起调用，除非该提供者挂了，再连另一台
	 * 粘滞连接将自动开启延迟连接，以减少长连接数
	 */
	private volatile Invoker<T> stickyInvoker = null;

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

	@Override
	public Class<T> getInterface() {
		return directory.getInterface();
	}

	@Override
	public URL getUrl() {
		return directory.getUrl();
	}

	@Override
	public boolean isAvailable() {
		Invoker<T> invoker = stickyInvoker;
		if (invoker != null) {
			return invoker.isAvailable();
		}
		return directory.isAvailable();
	}

	@Override
	public void destroy() {
		if (destroyed.compareAndSet(false, true)) {
			directory.destroy();
		}
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
	protected Invoker<T> select(LoadBalance loadbalance, Invocation invocation,
								List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {

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

	/**
	 * 选择invoker的核心规则
	 * @param selected 正常调用会传null，如失败重试等二次调用，会传调用时的invoker
	 */
	private Invoker<T> doSelect(LoadBalance loadbalance, Invocation invocation,
								List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {
		// 边界判断
		if (CollectionUtils.isEmpty(invokers)) {
			return null;
		}
		if (invokers.size() == 1) {
			return invokers.get(0);
		}
		// 调用AbstractLoadBalance#select()方法，从invoker列表中选择一个invoker
		Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);

		// 如果选出的invoker是已经选中的invoker
		// 或者再需要进行可用性检查的情况下，选出的invoker不可用
		if ((selected != null && selected.contains(invoker))
				|| (!invoker.isAvailable() && getUrl() != null && availablecheck)) {
			try {
				// 重新进行选取
				Invoker<T> rInvoker = reselect(loadbalance, invocation, invokers, selected, availablecheck);
				if (rInvoker != null) {
					invoker = rInvoker;
				} else {
					// 如果没有选取出合适的invoker，采用nextOne的方式
					int index = invokers.indexOf(invoker);
					try {
						// 避免碰撞，按照当前的nextOne的形式选取invoker，同时还避免了数组索引溢出
						invoker = invokers.get((index + 1) % invokers.size());
					} catch (Exception e) {
						logger.warn(e.getMessage() + " may because invokers list dynamic change, ignore.", e);
					}
				}
			} catch (Throwable t) {
				logger.error("cluster reselect fail reason is :" + t.getMessage() + " if can not solve, you can set cluster.availablecheck=false in url", t);
			}
		}
		return invoker;
	}

	/**
	 * 重新选取invoker
	 * 首先将使用不在已选invoker列表中的invoker，如果所有的invoker都在已选invoker列表中
	 * 那么只好使用负载均衡策略选取出一个可用的invoker
	 * @param loadbalance    load balance policy
	 * @param invocation     invocation
	 * @param invokers       invoker candidates
	 * @param selected       exclude selected invokers or not
	 * @param availablecheck check invoker available if true
	 * @return the reselect result to do invoke
	 * @throws RpcException exception
	 */
	private Invoker<T> reselect(LoadBalance loadbalance, Invocation invocation,
								List<Invoker<T>> invokers, List<Invoker<T>> selected, boolean availablecheck) throws RpcException {

		//Allocating one in advance, this list is certain to be used.
		List<Invoker<T>> reselectInvokers = new ArrayList<>(
				invokers.size() > 1 ? (invokers.size() - 1) : invokers.size());

		// 首先，尝试选取出不在已选invoker列表中的invoker
		for (Invoker<T> invoker : invokers) {
			if (availablecheck && !invoker.isAvailable()) {
				continue;
			}

			if (selected == null || !selected.contains(invoker)) {
				reselectInvokers.add(invoker);
			}
		}
		// 然后在非已选invoker列表中，使用负载均衡策略选取出一个invoker
		if (!reselectInvokers.isEmpty()) {
			return loadbalance.select(reselectInvokers, getUrl(), invocation);
		}

		// 此时invoker列表和已选invoker列表是元素的相同的集合
		// 先筛已选invoker列表中过滤出可用的invoker
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

		return null;
	}

	@Override
	public Result invoke(final Invocation invocation) throws RpcException {
		// 校验集群状态
		checkWhetherDestroyed();

		// 将调用过程上下文附加参数放入到invocation中
		Map<String, String> contextAttachments = RpcContext.getContext().getAttachments();
		if (contextAttachments != null && contextAttachments.size() != 0) {
			((RpcInvocation) invocation).addAttachments(contextAttachments);
		}
		// 获取invoker列表
		List<Invoker<T>> invokers = list(invocation);
		// 初始化负载均衡策略
		LoadBalance loadbalance = initLoadBalance(invokers, invocation);
		// 如果是异步调用，设置调用编号
		RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);
		// 根据同步的执行策略，执行调用
		return doInvoke(invocation, invokers, loadbalance);
	}

	protected void checkWhetherDestroyed() {
		if (destroyed.get()) {
			throw new RpcException("Rpc cluster invoker for " + getInterface() + " on consumer " + NetUtils.getLocalHost()
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
			throw new RpcException(RpcException.NO_INVOKER_AVAILABLE_AFTER_FILTER, "Failed to invoke the method "
					+ invocation.getMethodName() + " in the service " + getInterface().getName()
					+ ". No provider available for the service " + directory.getUrl().getServiceKey()
					+ " from registry " + directory.getUrl().getAddress()
					+ " on the consumer " + NetUtils.getLocalHost()
					+ " using the dubbo version " + Version.getVersion()
					+ ". Please check if the providers have been started and registered.");
		}
	}

	protected abstract Result doInvoke(Invocation invocation, List<Invoker<T>> invokers,
									   LoadBalance loadbalance) throws RpcException;

	protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
		return directory.list(invocation);
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
		if (CollectionUtils.isNotEmpty(invokers)) {
			return ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(invokers.get(0).getUrl()
					.getMethodParameter(RpcUtils.getMethodName(invocation), LOADBALANCE_KEY, DEFAULT_LOADBALANCE));
		} else {
			return ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(DEFAULT_LOADBALANCE);
		}
	}
}
