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
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WARMUP;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WEIGHT;
import static org.apache.dubbo.rpc.cluster.Constants.WARMUP_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

/**
 * AbstractLoadBalance
 * 负载均衡抽象类，提供权重计算功能
 */
public abstract class AbstractLoadBalance implements LoadBalance {
    /**
	 * 计算启动预热时的权重
	 * 新的权重的范围是1到weight的闭区间
     * @param uptime the uptime in milliseconds
     * @param warmup the warmup time in milliseconds
     * @param weight the weight of an invoker
     * @return weight which takes warmup into account
     */
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
		// 计算权重，其实相当于weight的时间百分比，直到达到weight，weight*(uptime/warmup)
		// 意思是，100的weight，如果需要预热10分钟，则每分钟增加10%的流量
		// 权重为[1, weight]
        int ww = (int) ( uptime / ((float) warmup / weight));
        return ww < 1 ? 1 : (Math.min(ww, weight));
    }

	/**
	 * 实现{@link LoadBalance}接口的select方法，根据不同的实现类，实现doSelect()核心方法
	 */
	@Override
	public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
		if (CollectionUtils.isEmpty(invokers)) {
			return null;
		}
		// 如果仅有一个调用者，没必要进行负载均衡
		if (invokers.size() == 1) {
			return invokers.get(0);
		}
		// 进行负载均衡，选择调用者
		return doSelect(invokers, url, invocation);
	}

    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);


    /**
	 * 获取调用invoker的权重
	 * 如果启动时间处于预热时间内，权重会按比例降低
     * @param invoker    the invoker
     * @param invocation the invocation of this invoker
     * @return weight
     */
    int getWeight(Invoker<?> invoker, Invocation invocation) {
		// 获取weight的配置，默认为100
        int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), WEIGHT_KEY, DEFAULT_WEIGHT);
        if (weight > 0) {
            long timestamp = invoker.getUrl().getParameter(TIMESTAMP_KEY, 0L);
            if (timestamp > 0L) {
				// 获取启动的总时长
                long uptime = System.currentTimeMillis() - timestamp;
				// 如果时间出现异常，weight为1
                if (uptime < 0) {
                    return 1;
				}
				// 获取预热的总时长
                int warmup = invoker.getUrl().getParameter(WARMUP_KEY, DEFAULT_WARMUP);
				// 如果处于预热中，则需要计算当前的权重
                if (uptime > 0 && uptime < warmup) {
                    weight = calculateWarmupWeight((int)uptime, warmup, weight);
                }
            }
		}
		// 否则取weight和0的最大值
        return Math.max(weight, 0);
    }
}
