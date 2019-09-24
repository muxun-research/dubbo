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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * InvokerHandler
 */
public class InvokerInvocationHandler implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(InvokerInvocationHandler.class);
	/**
	 * Invoker对象
	 */
	private final Invoker<?> invoker;

    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
	}

	/**
	 * 进行RPC调用
	 */
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		// 获取方法名称
		String methodName = method.getName();
		// 获取方法的参数类型
		Class<?>[] parameterTypes = method.getParameterTypes();
		// 处理wait()、notify()、notifyAll()等返回Object的方法
		if (method.getDeclaringClass() == Object.class) {
			return method.invoke(invoker, args);
		}
		// 处理toString()方法
		if ("toString".equals(methodName) && parameterTypes.length == 0) {
			return invoker.toString();
		}
		// 处理hashCode()方法
		if ("hashCode".equals(methodName) && parameterTypes.length == 0) {
			return invoker.hashCode();
		}
		// 处理equals()方法
		if ("equals".equals(methodName) && parameterTypes.length == 1) {
			return invoker.equals(args[0]);
		}

		return invoker.invoke(new RpcInvocation(method, args)).recreate();
	}
}
