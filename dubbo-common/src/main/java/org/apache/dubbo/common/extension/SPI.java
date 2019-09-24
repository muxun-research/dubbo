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

package org.apache.dubbo.common.extension;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * extension interface的制造者
 * Protocol接口为例，配置文件地址为META-INF/dubbo/com.xxx.Protocol
 * 如果有经静态字段或者扩展点实现的方法通过第三方依赖引用，如果第三方依赖不存在，那么这个扩展点就会初始化失败。
 * 在这种情况下，dubbo无法得出扩展点的ID，因此也不能映射扩展点的异常信息。
 * 如果加载mina扩展点失败了，并且用户配置使用了mina，dubbo将会认为扩展点没有被加载，而不是去报告mina这个扩展点实现提取失败和提取失败的原因
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface SPI {

    /**
	 * 默认的扩展点名称
     */
    String value() default "";

}