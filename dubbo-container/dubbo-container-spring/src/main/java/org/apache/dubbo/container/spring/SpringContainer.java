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
package org.apache.dubbo.container.spring;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.container.Container;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Spring服务容器实现，为现在Dubbo默认的服务容器实现
 */
public class SpringContainer implements Container {

	/**
	 * dubbo的Spring配置
	 */
	public static final String SPRING_CONFIG = "dubbo.spring.config";
	/**
	 * dubbo的默认配置文件地址
	 */
	public static final String DEFAULT_SPRING_CONFIG = "classpath*:META-INF/spring/*.xml";
    private static final Logger logger = LoggerFactory.getLogger(SpringContainer.class);
	/**
	 * Spring上下文，全局唯一
	 */
	static ClassPathXmlApplicationContext context;

    public static ClassPathXmlApplicationContext getContext() {
        return context;
    }

    @Override
    public void start() {
		// 获取Spring配置的配置路径
        String configPath = ConfigUtils.getProperty(SPRING_CONFIG);
		// 如果没有以"dubbo.spring.config"为开头的配置属性，则检查默认的配置文件地址
        if (StringUtils.isEmpty(configPath)) {
            configPath = DEFAULT_SPRING_CONFIG;
		}
		// 根据配置创建Spring上下文信息
        context = new ClassPathXmlApplicationContext(configPath.split("[,\\s]+"), false);
		// 刷新
        context.refresh();
		// 启动，并发送上下文初始化完成的事件
        context.start();
    }

    @Override
    public void stop() {
		try {
			// 停止并关闭Spring上下文
            if (context != null) {
                context.stop();
                context.close();
                context = null;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

}
