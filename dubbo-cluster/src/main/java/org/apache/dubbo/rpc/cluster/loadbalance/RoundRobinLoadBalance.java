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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-Robin负载均衡
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
	public static final String NAME = "roundrobin";

	private static final int RECYCLE_PERIOD = 60000;

	@Override
	protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
		// 首先获取服务的key
		// 因为invoker列表中的请求url是相同的，所以取第一个即可
		String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
		// 从全局的负载均衡中获取对应服务的负载均衡策略
		ConcurrentMap<String, WeightedRoundRobin> map = methodWeightMap.get(key);
		// 如果是刚启动的服务，创建一个新策略组
		if (map == null) {
			methodWeightMap.putIfAbsent(key, new ConcurrentHashMap<String, WeightedRoundRobin>());
			map = methodWeightMap.get(key);
		}

		int totalWeight = 0;
		long maxCurrent = Long.MIN_VALUE;
		long now = System.currentTimeMillis();
		Invoker<T> selectedInvoker = null;
		WeightedRoundRobin selectedWRR = null;
		// 遍历所有的调用者
		for (Invoker<T> invoker : invokers) {
			// 获取校验符
			String identifyString = invoker.getUrl().toIdentityString();
			// 获取当前invoker的roundrobin权重
			WeightedRoundRobin weightedRoundRobin = map.get(identifyString);
			// 调用AbstractLoadBalance#getWeight()方法，计算invoker的权重
			int weight = getWeight(invoker, invocation);
			// 如果当前invoker还没有roundrobin权重
			if (weightedRoundRobin == null) {
				// 创建全新的roundrobin权重
				weightedRoundRobin = new WeightedRoundRobin();
				// 设置权重并写入roundrobin负载均衡策略
				weightedRoundRobin.setWeight(weight);
				map.putIfAbsent(identifyString, weightedRoundRobin);
			}
			if (weight != weightedRoundRobin.getWeight()) {
				// 权重发生变化，更新权重
				weightedRoundRobin.setWeight(weight);
			}
			// 计数器+1
			long cur = weightedRoundRobin.increaseCurrent();
			// 最后一次更新时间
			weightedRoundRobin.setLastUpdate(now);
			// 如果已经超过计数阈值
			if (cur > maxCurrent) {
				// 设置当前的最大值
				// 如果已经溢出，不会更新下面的内容
				maxCurrent = cur;
				// 选中的invoker
				selectedInvoker = invoker;
				// 选中的负载均衡权重
				selectedWRR = weightedRoundRobin;
			}
			// 总权重
			totalWeight += weight;
		}
		// 更新所没有被占有，并且调用者和负载均衡策略记录的不均衡
		// 出现不均衡的情况示例：服务刚刚启动时
		if (!updateLock.get() && invokers.size() != map.size()) {
			// 占有更新锁
			if (updateLock.compareAndSet(false, true)) {
				try {
					// 复制一份出来，以备修改
					ConcurrentMap<String, WeightedRoundRobin> newMap = new ConcurrentHashMap<>(map);
					// 去除超过循环未轮询到的负载均衡配置
					newMap.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > RECYCLE_PERIOD);
					// 更新负载均衡配置策略
					methodWeightMap.put(key, newMap);
				} finally {
					// 释放更新锁
					updateLock.set(false);
				}
			}
		}
		// 如果已经选中了
		if (selectedInvoker != null) {
			// 更新权重总和，返回选中的invoker
			selectedWRR.sel(totalWeight);
			return selectedInvoker;
		}
		// 即使仅有一个invoker，也不会走到这里
		return invokers.get(0);
	}

	private ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<String, ConcurrentMap<String, WeightedRoundRobin>>();
	private AtomicBoolean updateLock = new AtomicBoolean();

	/**
	 * get invoker addr list cached for specified invocation
	 * <p>
	 * <b>for unit test only</b>
	 * @param invokers
	 * @param invocation
	 * @return
	 */
	protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
		String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
		Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
		if (map != null) {
			return map.keySet();
		}
		return null;
	}

	/**
	 * 包含权重的RoundRobin策略
	 */
	protected static class WeightedRoundRobin {
		/**
		 * 权重
		 */
		private int weight;
		/**
		 * 当前值
		 */
		private AtomicLong current = new AtomicLong(0);
		/**
		 * 最后一次更新时间戳
		 */
		private long lastUpdate;

		public int getWeight() {
			return weight;
		}

		public void setWeight(int weight) {
			this.weight = weight;
			current.set(0);
		}

		public long increaseCurrent() {
			return current.addAndGet(weight);
		}

		public void sel(int total) {
			current.addAndGet(-1 * total);
		}

		public long getLastUpdate() {
			return lastUpdate;
		}

		public void setLastUpdate(long lastUpdate) {
			this.lastUpdate = lastUpdate;
		}
	}

}
