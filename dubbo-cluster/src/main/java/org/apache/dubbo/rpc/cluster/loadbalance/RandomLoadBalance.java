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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机从invoker列表中获取一个invoker
 * 如果所有权重均是相同的，将采用random.nextInt(invokers.size())方法
 * 如果权重是不相同的，则采用random.nextInt(w1+w2+...+wn)方法
 * 如果机器性能是不同的，你可以为机器性能好的设置一个更高的权重
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    /**
     * Select one invoker between a list using a random criteria
     * @param invokers List of possible invokers
     * @param url URL
     * @param invocation Invocation
     * @param <T>
     * @return The selected invoker
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
		// invoker的数量
        int length = invokers.size();
		// 是否每个invoker拥有相同的权重，默认拥有相同的权重
        boolean sameWeight = true;
		// 用于记录每个invoker的权重
        int[] weights = new int[length];
		// 获取并设置权重
        int firstWeight = getWeight(invokers.get(0), invocation);
        weights[0] = firstWeight;
		// 以及计算权重的总和
        int totalWeight = firstWeight;
        for (int i = 1; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            weights[i] = weight;
            totalWeight += weight;
			// 取出第一个的原因是将以第一个invoker的权重作为标杆，校验权重是否是不相同的
			// 有一个权重不相同，即视为不相同，接下来不会进行相同的赋值操作
            if (sameWeight && weight != firstWeight) {
                sameWeight = false;
            }
        }
		// 处理invoker权重不同的情况
        if (totalWeight > 0 && !sameWeight) {
			// 则取一个权重随机数，然后看落入到哪个invoker权重区间内，理论上，权重越大，区间越大，被选中的几率越高
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < length; i++) {
                offset -= weights[i];
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }
		// 如果所有invoker的权重相同，或者权重之和为0，则随机从invoker中选择一个
        return invokers.get(ThreadLocalRandom.current().nextInt(length));
    }

}
