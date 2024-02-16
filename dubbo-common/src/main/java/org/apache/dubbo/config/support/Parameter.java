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
package org.apache.dubbo.config.support;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Parameter
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Parameter {

    /**
     * key，键名
     * 例如C
     */
    String key() default "";

    /**
     * 是否必填
     * 默认false
     */
    boolean required() default false;

    /**
     * 是否例外
     */
    boolean excluded() default false;

    /**
     * 是否包
     */
    boolean escaped() default false;

    /**
     * 是否为属性
     */
    boolean attribute() default true;

    /**
     * 是否拼接默认属性
     * 	 * 例如：ServiceConfig和ProviderConfig都继承自AbstractServiceConfig，所以ServiceConfig会继承ServiceConfig的值
     * 	 * 如果ProviderConfig.filter=firstFilter，ServiceConfig.filter=secondFilter，最终暴露到dubbo url时service.filter=firstFilter,secondFilter
     */
    boolean append() default false;
}
