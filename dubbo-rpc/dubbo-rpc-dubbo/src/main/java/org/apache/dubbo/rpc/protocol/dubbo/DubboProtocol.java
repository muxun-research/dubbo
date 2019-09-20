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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.serialize.support.SerializableClassRegistry;
import org.apache.dubbo.common.serialize.support.SerializationOptimizer;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Transporter;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchangers;
import org.apache.dubbo.remoting.exchange.support.ExchangeHandlerAdapter;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.AbstractProtocol;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.remoting.Constants.CHANNEL_READONLYEVENT_SENT_KEY;
import static org.apache.dubbo.remoting.Constants.CLIENT_KEY;
import static org.apache.dubbo.remoting.Constants.CODEC_KEY;
import static org.apache.dubbo.remoting.Constants.CONNECTIONS_KEY;
import static org.apache.dubbo.remoting.Constants.DEFAULT_HEARTBEAT;
import static org.apache.dubbo.remoting.Constants.DEFAULT_REMOTING_CLIENT;
import static org.apache.dubbo.remoting.Constants.HEARTBEAT_KEY;
import static org.apache.dubbo.remoting.Constants.SERVER_KEY;
import static org.apache.dubbo.rpc.Constants.DEFAULT_REMOTING_SERVER;
import static org.apache.dubbo.rpc.Constants.DEFAULT_STUB_EVENT;
import static org.apache.dubbo.rpc.Constants.IS_SERVER_KEY;
import static org.apache.dubbo.rpc.Constants.LAZY_CONNECT_KEY;
import static org.apache.dubbo.rpc.Constants.STUB_EVENT_KEY;
import static org.apache.dubbo.rpc.Constants.STUB_EVENT_METHODS_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.CALLBACK_SERVICE_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DEFAULT_SHARE_CONNECTIONS;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.IS_CALLBACK_SERVICE;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.ON_CONNECT_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.ON_DISCONNECT_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.OPTIMIZER_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.SHARE_CONNECTIONS_KEY;


/**
 * dubbo protocol support.
 */
public class DubboProtocol extends AbstractProtocol {

    public static final String NAME = "dubbo";

    public static final int DEFAULT_PORT = 20880;
    private static final String IS_CALLBACK_SERVICE_INVOKE = "_isCallBackServiceInvoke";
    private static DubboProtocol INSTANCE;

	/**
	 * 请求的服务器集合
	 * (host:ip, ExchangeServer)
	 */
    private final Map<String, ExchangeServer> serverMap = new ConcurrentHashMap<>();
	/**
	 * (host:ip, List<ReferenceCountExchangeClient>)
	 */
    private final Map<String, List<ReferenceCountExchangeClient>> referenceClientMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Object> locks = new ConcurrentHashMap<>();
    private final Set<String> optimizers = new ConcurrentHashSet<>();
    /**
     * consumer side export a stub service for dispatching event
     * servicekey-stubmethods
     */
    private final ConcurrentMap<String, String> stubServiceMethodsMap = new ConcurrentHashMap<>();

	/**
	 * dubbo自己实现的ExchangeHandler
	 */
	private ExchangeHandler requestHandler = new ExchangeHandlerAdapter() {

		/**
		 * 处理消费者远程调用
		 */
		@Override
		public CompletableFuture<Object> reply(ExchangeChannel channel, Object message) throws RemotingException {
			// message类型异常
			if (!(message instanceof Invocation)) {
				throw new RemotingException(channel, "Unsupported request: "
						+ (message == null ? null : (message.getClass().getName() + ": " + message))
						+ ", channel: consumer: " + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress());
			}

			Invocation inv = (Invocation) message;
			// 获取invoker
			Invoker<?> invoker = getInvoker(channel, inv);
			// 需要兼容之前的版本
			if (Boolean.TRUE.toString().equals(inv.getAttachments().get(IS_CALLBACK_SERVICE_INVOKE))) {
				// 获取方法
				String methodsStr = invoker.getUrl().getParameters().get("methods");
				boolean hasMethod = false;
				// 如果url中没有方法名称
				if (methodsStr == null || !methodsStr.contains(",")) {
					// 判断
					hasMethod = inv.getMethodName().equals(methodsStr);
				} else {
					// 存在多个调用方法，
					String[] methods = methodsStr.split(",");
					for (String method : methods) {
						if (inv.getMethodName().equals(method)) {
							hasMethod = true;
							break;
						}
					}
				}
				if (!hasMethod) {
					logger.warn(new IllegalStateException("The methodName " + inv.getMethodName()
							+ " not found in callback service interface ,invoke will be ignored."
							+ " please update the api interface. url is:"
							+ invoker.getUrl()) + " ,invocation is :" + inv);
					return null;
				}
			}
			RpcContext.getContext().setRemoteAddress(channel.getRemoteAddress());
			Result result = invoker.invoke(inv);
			return result.completionFuture().thenApply(Function.identity());
		}

		@Override
		public void received(Channel channel, Object message) throws RemotingException {
			if (message instanceof Invocation) {
				reply((ExchangeChannel) channel, message);

			} else {
				super.received(channel, message);
			}
		}

		@Override
		public void connected(Channel channel) throws RemotingException {
			invoke(channel, ON_CONNECT_KEY);
		}

		@Override
		public void disconnected(Channel channel) throws RemotingException {
			if (logger.isDebugEnabled()) {
				logger.debug("disconnected from " + channel.getRemoteAddress() + ",url:" + channel.getUrl());
			}
			invoke(channel, ON_DISCONNECT_KEY);
		}

		private void invoke(Channel channel, String methodKey) {
			Invocation invocation = createInvocation(channel, channel.getUrl(), methodKey);
			if (invocation != null) {
				try {
					received(channel, invocation);
				} catch (Throwable t) {
					logger.warn("Failed to invoke event method " + invocation.getMethodName() + "(), cause: " + t.getMessage(), t);
				}
			}
		}

		private Invocation createInvocation(Channel channel, URL url, String methodKey) {
			String method = url.getParameter(methodKey);
			if (method == null || method.length() == 0) {
				return null;
			}

			RpcInvocation invocation = new RpcInvocation(method, new Class<?>[0], new Object[0]);
			invocation.setAttachment(PATH_KEY, url.getPath());
			invocation.setAttachment(GROUP_KEY, url.getParameter(GROUP_KEY));
			invocation.setAttachment(INTERFACE_KEY, url.getParameter(INTERFACE_KEY));
			invocation.setAttachment(VERSION_KEY, url.getParameter(VERSION_KEY));
			if (url.getParameter(STUB_EVENT_KEY, false)) {
				invocation.setAttachment(STUB_EVENT_KEY, Boolean.TRUE.toString());
			}

			return invocation;
		}
	};

    public DubboProtocol() {
        INSTANCE = this;
    }

    public static DubboProtocol getDubboProtocol() {
        if (INSTANCE == null) {
            // load
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(DubboProtocol.NAME);
        }

        return INSTANCE;
    }

    public Collection<ExchangeServer> getServers() {
        return Collections.unmodifiableCollection(serverMap.values());
    }

    public Collection<Exporter<?>> getExporters() {
        return Collections.unmodifiableCollection(exporterMap.values());
    }

    Map<String, Exporter<?>> getExporterMap() {
        return exporterMap;
    }

    private boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(channel.getUrl().getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

	/**
	 * 获取调用Invoker
	 */
	Invoker<?> getInvoker(Channel channel, Invocation inv) throws RemotingException {
		// 是否需要进行回调
		boolean isCallBackServiceInvoke = false;
		// 是否需要进行本地存储
		boolean isStubServiceInvoke = false;
		// 获取本地请求的地址
		int port = channel.getLocalAddress().getPort();
		//
		String path = inv.getAttachments().get(PATH_KEY);

		// if it's callback service on client side
		isStubServiceInvoke = Boolean.TRUE.toString().equals(inv.getAttachments().get(STUB_EVENT_KEY));
		if (isStubServiceInvoke) {
			port = channel.getRemoteAddress().getPort();
		}

		//callback
		isCallBackServiceInvoke = isClientSide(channel) && !isStubServiceInvoke;
		if (isCallBackServiceInvoke) {
			path += "." + inv.getAttachments().get(CALLBACK_SERVICE_KEY);
			inv.getAttachments().put(IS_CALLBACK_SERVICE_INVOKE, Boolean.TRUE.toString());
		}
		// 获取服务key
		String serviceKey = serviceKey(port, path, inv.getAttachments().get(VERSION_KEY), inv.getAttachments().get(GROUP_KEY));
		// 从缓存的exporterMap中获取key
		DubboExporter<?> exporter = (DubboExporter<?>) exporterMap.get(serviceKey);

		if (exporter == null) {
			throw new RemotingException(channel, "Not found exported service: " + serviceKey + " in " + exporterMap.keySet() + ", may be version or group mismatch " +
					", channel: consumer: " + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress() + ", message:" + inv);
		}
		// DubboExporter中获取invoker
		return exporter.getInvoker();
	}

    public Collection<Invoker<?>> getInvokers() {
        return Collections.unmodifiableCollection(invokers);
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
		// 获取请求url
        URL url = invoker.getUrl();

		// 获取服务的key
        String key = serviceKey(url);
		// 创建DubboExporter
        DubboExporter<T> exporter = new DubboExporter<T>(invoker, key, exporterMap);
        exporterMap.put(key, exporter);

        //export an stub service for dispatching event
        Boolean isStubSupportEvent = url.getParameter(STUB_EVENT_KEY, DEFAULT_STUB_EVENT);
        Boolean isCallbackservice = url.getParameter(IS_CALLBACK_SERVICE, false);
        if (isStubSupportEvent && !isCallbackservice) {
            String stubServiceMethods = url.getParameter(STUB_EVENT_METHODS_KEY);
            if (stubServiceMethods == null || stubServiceMethods.length() == 0) {
                if (logger.isWarnEnabled()) {
                    logger.warn(new IllegalStateException("consumer [" + url.getParameter(INTERFACE_KEY) +
                            "], has set stubproxy support event ,but no stub methods founded."));
                }

            } else {
                stubServiceMethodsMap.put(url.getServiceKey(), stubServiceMethods);
            }
		}
		// 启动服务
        openServer(url);
		// 优化序列化器
        optimizeSerialization(url);

        return exporter;
    }

    private void openServer(URL url) {
		// 示例：dubbo://172.19.164.140:20880/
		// com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService?
		// anyhost=true&application=dubbo-client-first&bean.name=ServiceBean:com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService
		// &bind.ip=172.19.164.140&bind.port=20880&deprecated=false&dubbo=2.0.2&dynamic=true&generic=false&interface=com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService&methods=shine
		// &pid=10033&qos.enable=false
		// &register=true&release=2.7.3&side=provider&timestamp=1568885985111
		// 获取host:ip部分
        String key = url.getAddress();
		// 只有provider才能暴露服务，进行一次校验
        boolean isServer = url.getParameter(IS_SERVER_KEY, true);
        if (isServer) {
			// 查看缓存中是有指定的key
            ExchangeServer server = serverMap.get(key);
            if (server == null) {
				// 存在并发的情况，加双重检验锁
                synchronized (this) {
                    server = serverMap.get(key);
                    if (server == null) {
						// 创建一个ExchangeServer，放入缓存中
                        serverMap.put(key, createServer(url));
                    }
                }
            } else {
                // server supports reset, use together with override
                server.reset(url);
            }
        }
    }

    private ExchangeServer createServer(URL url) {
		// 添加一些新的参数，重新构造
        url = URLBuilder.from(url)
				// 当服务器关闭时，仍然允许发送只读事件
                .addParameterIfAbsent(CHANNEL_READONLYEVENT_SENT_KEY, Boolean.TRUE.toString())
				// 添加心跳机制，心跳机制60s
                .addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT))
				//
                .addParameter(CODEC_KEY, DubboCodec.NAME)
                .build();
		// 示例：
		// dubbo://172.19.164.140:20880/com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService?
		// anyhost=true&application=dubbo-client-first&bean.name=ServiceBean:com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService
		// &bind.ip=172.19.164.140&bind.port=20880
		// &channel.readonly.sent=true
		// &codec=dubbo
		// &deprecated=false&dubbo=2.0.2&dynamic=true&generic=false
		// &heartbeat=60000
		// &interface=com.sunshine.service.spring.cloud.alibaba.laboratory.dubbo.api.service.SunshineService&methods=shine&pid=10033
		// &qos.enable=false&register=true&release=2.7.3&side=provider&timestamp=1568885985111
		// 获取Server类型，默认为Netty
        String str = url.getParameter(SERVER_KEY, DEFAULT_REMOTING_SERVER);
		// 如果传输类型dubbo不支持，或者开发者没有实现，抛出异常
        if (str != null && str.length() > 0 && !ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(str)) {
            throw new RpcException("Unsupported server type: " + str + ", url: " + url);
        }

        ExchangeServer server;
		try {
			// 构建并启动服务器
            server = Exchangers.bind(url, requestHandler);
        } catch (RemotingException e) {
            throw new RpcException("Fail to start server(url: " + url + ") " + e.getMessage(), e);
        }

        str = url.getParameter(CLIENT_KEY);
        if (str != null && str.length() > 0) {
            Set<String> supportedTypes = ExtensionLoader.getExtensionLoader(Transporter.class).getSupportedExtensions();
            if (!supportedTypes.contains(str)) {
                throw new RpcException("Unsupported client type: " + str);
            }
        }

        return server;
	}

	/**
	 * 初始化序列化器
	 * @param url provider服务url
	 */
	private void optimizeSerialization(URL url) throws RpcException {
		// 获取优化器，默认为""
		String className = url.getParameter(OPTIMIZER_KEY, "");
		// 如果没有指定优化器，或者已经注册序列化器，不继续进行初始化
		if (StringUtils.isEmpty(className) || optimizers.contains(className)) {
			return;
		}

		logger.info("Optimizing the serialization process for Kryo, FST, etc...");

		try {
			// 加载SerializationOptimizer实现类
			Class clazz = Thread.currentThread().getContextClassLoader().loadClass(className);

			if (!SerializationOptimizer.class.isAssignableFrom(clazz)) {
				throw new RpcException("The serialization optimizer " + className + " isn't an instance of " + SerializationOptimizer.class.getName());
			}
			// 创建指定SerializationOptimizer实例
			SerializationOptimizer optimizer = (SerializationOptimizer) clazz.newInstance();
			// 如果需要序列化的类为null，不进行初始化
			if (optimizer.getSerializableClasses() == null) {
				return;
			}
			// 获取需要进行序列化的类，注册需要序列化的类
			for (Class c : optimizer.getSerializableClasses()) {
				SerializableClassRegistry.registerClass(c);
			}
			// 当前服务调用也存储一份序列化类
			optimizers.add(className);

		} catch (ClassNotFoundException e) {
			throw new RpcException("Cannot find the serialization optimizer class: " + className, e);

		} catch (InstantiationException e) {
			throw new RpcException("Cannot instantiate the serialization optimizer class: " + className, e);

		} catch (IllegalAccessException e) {
			throw new RpcException("Cannot instantiate the serialization optimizer class: " + className, e);
		}
	}

    @Override
    public <T> Invoker<T> protocolBindingRefer(Class<T> serviceType, URL url) throws RpcException {
        optimizeSerialization(url);

        // create rpc invoker.
        DubboInvoker<T> invoker = new DubboInvoker<T>(serviceType, url, getClients(url), invokers);
        invokers.add(invoker);

        return invoker;
    }

    private ExchangeClient[] getClients(URL url) {
        // whether to share connection

        boolean useShareConnect = false;

        int connections = url.getParameter(CONNECTIONS_KEY, 0);
        List<ReferenceCountExchangeClient> shareClients = null;
        // if not configured, connection is shared, otherwise, one connection for one service
        if (connections == 0) {
            useShareConnect = true;

            /**
             * The xml configuration should have a higher priority than properties.
             */
            String shareConnectionsStr = url.getParameter(SHARE_CONNECTIONS_KEY, (String) null);
            connections = Integer.parseInt(StringUtils.isBlank(shareConnectionsStr) ? ConfigUtils.getProperty(SHARE_CONNECTIONS_KEY,
                    DEFAULT_SHARE_CONNECTIONS) : shareConnectionsStr);
            shareClients = getSharedClient(url, connections);
        }

        ExchangeClient[] clients = new ExchangeClient[connections];
        for (int i = 0; i < clients.length; i++) {
            if (useShareConnect) {
                clients[i] = shareClients.get(i);

            } else {
                clients[i] = initClient(url);
            }
        }

        return clients;
    }

    /**
     * Get shared connection
     *
     * @param url
     * @param connectNum connectNum must be greater than or equal to 1
     */
    private List<ReferenceCountExchangeClient> getSharedClient(URL url, int connectNum) {
        String key = url.getAddress();
        List<ReferenceCountExchangeClient> clients = referenceClientMap.get(key);

        if (checkClientCanUse(clients)) {
            batchClientRefIncr(clients);
            return clients;
        }

        locks.putIfAbsent(key, new Object());
        synchronized (locks.get(key)) {
            clients = referenceClientMap.get(key);
            // dubbo check
            if (checkClientCanUse(clients)) {
                batchClientRefIncr(clients);
                return clients;
            }

            // connectNum must be greater than or equal to 1
            connectNum = Math.max(connectNum, 1);

            // If the clients is empty, then the first initialization is
            if (CollectionUtils.isEmpty(clients)) {
                clients = buildReferenceCountExchangeClientList(url, connectNum);
                referenceClientMap.put(key, clients);

            } else {
                for (int i = 0; i < clients.size(); i++) {
                    ReferenceCountExchangeClient referenceCountExchangeClient = clients.get(i);
                    // If there is a client in the list that is no longer available, create a new one to replace him.
                    if (referenceCountExchangeClient == null || referenceCountExchangeClient.isClosed()) {
                        clients.set(i, buildReferenceCountExchangeClient(url));
                        continue;
                    }

                    referenceCountExchangeClient.incrementAndGetCount();
                }
            }

            /**
             * I understand that the purpose of the remove operation here is to avoid the expired url key
             * always occupying this memory space.
             */
            locks.remove(key);

            return clients;
        }
    }

    /**
     * Check if the client list is all available
     *
     * @param referenceCountExchangeClients
     * @return true-available，false-unavailable
     */
    private boolean checkClientCanUse(List<ReferenceCountExchangeClient> referenceCountExchangeClients) {
        if (CollectionUtils.isEmpty(referenceCountExchangeClients)) {
            return false;
        }

        for (ReferenceCountExchangeClient referenceCountExchangeClient : referenceCountExchangeClients) {
            // As long as one client is not available, you need to replace the unavailable client with the available one.
            if (referenceCountExchangeClient == null || referenceCountExchangeClient.isClosed()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Increase the reference Count if we create new invoker shares same connection, the connection will be closed without any reference.
     *
     * @param referenceCountExchangeClients
     */
    private void batchClientRefIncr(List<ReferenceCountExchangeClient> referenceCountExchangeClients) {
        if (CollectionUtils.isEmpty(referenceCountExchangeClients)) {
            return;
        }

        for (ReferenceCountExchangeClient referenceCountExchangeClient : referenceCountExchangeClients) {
            if (referenceCountExchangeClient != null) {
                referenceCountExchangeClient.incrementAndGetCount();
            }
        }
    }

    /**
     * Bulk build client
     *
     * @param url
     * @param connectNum
     * @return
     */
    private List<ReferenceCountExchangeClient> buildReferenceCountExchangeClientList(URL url, int connectNum) {
        List<ReferenceCountExchangeClient> clients = new ArrayList<>();

        for (int i = 0; i < connectNum; i++) {
            clients.add(buildReferenceCountExchangeClient(url));
        }

        return clients;
    }

    /**
     * Build a single client
     *
     * @param url
     * @return
     */
    private ReferenceCountExchangeClient buildReferenceCountExchangeClient(URL url) {
        ExchangeClient exchangeClient = initClient(url);

        return new ReferenceCountExchangeClient(exchangeClient);
    }

    /**
     * Create new connection
     *
     * @param url
     */
    private ExchangeClient initClient(URL url) {

        // client type setting.
        String str = url.getParameter(CLIENT_KEY, url.getParameter(SERVER_KEY, DEFAULT_REMOTING_CLIENT));

        url = url.addParameter(CODEC_KEY, DubboCodec.NAME);
        // enable heartbeat by default
        url = url.addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT));

        // BIO is not allowed since it has severe performance issue.
        if (str != null && str.length() > 0 && !ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(str)) {
            throw new RpcException("Unsupported client type: " + str + "," +
                    " supported client type is " + StringUtils.join(ExtensionLoader.getExtensionLoader(Transporter.class).getSupportedExtensions(), " "));
        }

        ExchangeClient client;
        try {
            // connection should be lazy
            if (url.getParameter(LAZY_CONNECT_KEY, false)) {
                client = new LazyConnectExchangeClient(url, requestHandler);

            } else {
                client = Exchangers.connect(url, requestHandler);
            }

        } catch (RemotingException e) {
            throw new RpcException("Fail to create remoting client for service(" + url + "): " + e.getMessage(), e);
        }

        return client;
    }

    @Override
    public void destroy() {
        for (String key : new ArrayList<>(serverMap.keySet())) {
            ExchangeServer server = serverMap.remove(key);

            if (server == null) {
                continue;
            }

            try {
                if (logger.isInfoEnabled()) {
                    logger.info("Close dubbo server: " + server.getLocalAddress());
                }

                server.close(ConfigurationUtils.getServerShutdownTimeout());

            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }

        for (String key : new ArrayList<>(referenceClientMap.keySet())) {
            List<ReferenceCountExchangeClient> clients = referenceClientMap.remove(key);

            if (CollectionUtils.isEmpty(clients)) {
                continue;
            }

            for (ReferenceCountExchangeClient client : clients) {
                closeReferenceCountExchangeClient(client);
            }
        }

        stubServiceMethodsMap.clear();
        super.destroy();
    }

    /**
     * close ReferenceCountExchangeClient
     *
     * @param client
     */
    private void closeReferenceCountExchangeClient(ReferenceCountExchangeClient client) {
        if (client == null) {
            return;
        }

        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close dubbo connect: " + client.getLocalAddress() + "-->" + client.getRemoteAddress());
            }

            client.close(ConfigurationUtils.getServerShutdownTimeout());

            // TODO
            /**
             * At this time, ReferenceCountExchangeClient#client has been replaced with LazyConnectExchangeClient.
             * Do you need to call client.close again to ensure that LazyConnectExchangeClient is also closed?
             */

        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }
}
