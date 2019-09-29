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

import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FORKS;
import static org.apache.dubbo.rpc.cluster.Constants.FORKS_KEY;

/**
 * 注意：异步情况下工作并非良好
 * 并发调用指定数目的invoker，通常用于要求较高的实时操作，但是需要占用更多的资源
 */
public class ForkingClusterInvoker<T> extends AbstractClusterInvoker<T> {

    /**
     * Use {@link NamedInternalThreadFactory} to produce {@link org.apache.dubbo.common.threadlocal.InternalThread}
     * which with the use of {@link org.apache.dubbo.common.threadlocal.InternalThreadLocal} in {@link RpcContext}.
     */
    private final ExecutorService executor = Executors.newCachedThreadPool(
            new NamedInternalThreadFactory("forking-cluster-timer", true));

    public ForkingClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        try {
			// invoker校验
            checkInvokers(invokers, invocation);
			// 选取出的invoker
            final List<Invoker<T>> selected;
			// 获取设置的最大并行数，默认为2
            final int forks = getUrl().getParameter(FORKS_KEY, DEFAULT_FORKS);
			// 获取调用的超时时间
            final int timeout = getUrl().getParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
			// 如果不需要并行，或者允许的最大并行数量超过了invoker的数量
            if (forks <= 0 || forks >= invokers.size()) {
                selected = invokers;
            } else {
				// 如果并行数量小于invoker的数量，且需要进行并行
                selected = new ArrayList<>();
                for (int i = 0; i < forks; i++) {
					// 选取出invoker
                    Invoker<T> invoker = select(loadbalance, invocation, invokers, selected);
					// 避免重复添加
                    if (!selected.contains(invoker)) {
                        //Avoid add the same invoker several times.
                        selected.add(invoker);
                    }
                }
            }
			// 调用上线文中设置invoker信息
            RpcContext.getContext().setInvokers((List) selected);
			// 计数器
            final AtomicInteger count = new AtomicInteger();
			// 创建LinkedBlockingQueue
            final BlockingQueue<Object> ref = new LinkedBlockingQueue<>();
			// 遍历选取出的invoker
            for (final Invoker<T> invoker : selected) {
				// 分别让每个invoker都执行相同的任务，并放入到队列中
                executor.execute(() -> {
                    try {
                        Result result = invoker.invoke(invocation);
                        ref.offer(result);
                    } catch (Throwable e) {
						// 计数器操作
                        int value = count.incrementAndGet();
						// 如果invoker调用出现了异常，将异常添加到队列中
                        if (value >= selected.size()) {
                            ref.offer(e);
                        }
                    }
                });
            }
            try {
				// 阻塞等待队列中调用结果
                Object ret = ref.poll(timeout, TimeUnit.MILLISECONDS);
				// 如果调用先捕获到了异常，直接抛出异常
                if (ret instanceof Throwable) {
                    Throwable e = (Throwable) ret;
                    throw new RpcException(e instanceof RpcException ? ((RpcException) e).getCode() : 0, "Failed to forking invoke provider " + selected + ", but no luck to perform the invocation. Last error is: " + e.getMessage(), e.getCause() != null ? e.getCause() : e);
                }
				// 否则获取第一个执行成功的结果
                return (Result) ret;
            } catch (InterruptedException e) {
                throw new RpcException("Failed to forking invoke provider " + selected + ", but no luck to perform the invocation. Last error is: " + e.getMessage(), e);
            }
        } finally {
			// 执行完成了，删除绑定在当前线程的附加参数
            RpcContext.getContext().clearAttachments();
        }
    }
}
