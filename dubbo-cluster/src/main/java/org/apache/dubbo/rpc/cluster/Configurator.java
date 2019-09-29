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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.rpc.cluster.Constants.PRIORITY_KEY;

/**
 * 配置规则接口
 * 一个Configurator对象，对应一条规则
 * 使用SPI进行加载，多例，线程安全
 */
public interface Configurator extends Comparable<Configurator> {

	/**
	 * 将复写的URL转换为map，用于重新refer()使用
	 * 每次将会下发全部的规则，所有的URL都会进行重新组装和计算
	 * URL契约:
	 * override://0.0.0.0/...(或者override://ip:port...?anyhost=true)&para1=value1...代表全局规则（对所有的provider均生效）
	 * override://ip:port...?anyhost=false，代表特殊规则，仅对某个的provider有效
	 * override://不支持此规则，需要注册中心自行计算
	 * 无参的override://0.0.0.0/，代表需要清除override
	 * @param urls 需要进行转换的URL列表
	 * @return 转换的配置规则列表
	 */
	static Optional<List<Configurator>> toConfigurators(List<URL> urls) {
		if (CollectionUtils.isEmpty(urls)) {
			return Optional.empty();
		}

		ConfiguratorFactory configuratorFactory = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class)
				.getAdaptiveExtension();

		List<Configurator> configurators = new ArrayList<>(urls.size());
		for (URL url : urls) {
			// 如果协议为"empty://"，代表需要清空所有的配置，因此返回空配置信息集合
			if (EMPTY_PROTOCOL.equals(url.getProtocol())) {
				configurators.clear();
				break;
			}
			// 获取URL中的参数
			Map<String, String> override = new HashMap<>(url.getParameters());
			// override中的anyhost键值对可能会自动添加，但是不能作为判断URL发生变化的依据
			override.remove(ANYHOST_KEY);
			// 对应第四条契约，不带参数的URL，标识清除URL
			if (override.size() == 0) {
				configurators.clear();
				continue;
			}
			// 使用配置工厂创建配置信息，并添加到配置信息集合中
			configurators.add(configuratorFactory.getConfigurator(url));
		}
		// 排序，见#compareTo()方法
		Collections.sort(configurators);
		return Optional.of(configurators);
	}

	/**
	 * 获取配置的URL
	 * @return configurator url.
	 */
	URL getUrl();

	/**
	 * 配置provider的url
	 * @param url 旧provider的URL
	 * @return 新provider的URL
	 */
	URL configure(URL url);

	/**
	 * 先根据host进行排序，再根据优先级进行排序
	 * 1. 带有指定host ip地址的URL，拥有比0.0.0.0更高的优先级
	 * 2. 如果两个URL拥有相同的host，则对游侠难吃进行比较
	 */
	@Override
	default int compareTo(Configurator o) {
		if (o == null) {
			return -1;
		}

		int ipCompare = getUrl().getHost().compareTo(o.getUrl().getHost());
		// host相同，使用优先级进行排序
		if (ipCompare == 0) {
			int i = getUrl().getParameter(PRIORITY_KEY, 0);
			int j = o.getUrl().getParameter(PRIORITY_KEY, 0);
			return Integer.compare(i, j);
		} else {
			return ipCompare;
		}
	}
}
