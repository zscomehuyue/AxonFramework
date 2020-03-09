/*
 * Copyright (c) 2010-2019. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.springboot.autoconfig;

import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.config.EventProcessingModule;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto configuration for {@link EventProcessingModule}.
 * FIXME 事件处理配置，需要存储数据；存储使用的orm，是hibernate；
 * FIXME JPA配置
 *      <dependency>
 *         <groupId>org.springframework.boot</groupId>
 *         <artifactId>spring-boot-starter-data-jpa</artifactId>
 *      </dependency>
 *
 *
 * @author Milan Savic
 * @since 4.0
 */
@Configuration
@AutoConfigureAfter(name = {
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
        "org.axonframework.springboot.autoconfig.JpaAutoConfiguration",
        "org.axonframework.springboot.autoconfig.JdbcAutoConfiguration",
        "org.axonframework.springboot.autoconfig.JpaEventStoreAutoConfiguration",
        "org.axonframework.springboot.autoconfig.ObjectMapperAutoConfiguration"
})
public class EventProcessingAutoConfiguration {

    /**
     *  FIXME 事件处理模块，注册所有的配置组件，及依赖组件，延迟初始化；
     */
    @Bean
    @ConditionalOnMissingBean({EventProcessingModule.class, EventProcessingConfiguration.class})
    public EventProcessingModule eventProcessingModule() {
        return new EventProcessingModule();
    }
}
