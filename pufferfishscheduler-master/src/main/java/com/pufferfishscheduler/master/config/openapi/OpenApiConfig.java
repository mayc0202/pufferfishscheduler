package com.pufferfishscheduler.master.config.openapi;

import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenApi 配置类
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        final String securitySchemeName = "bearerAuth";

        return new OpenAPI()
                .info(new Info()
                        .title("Pufferfish Scheduler API")
                        .description("""
                                <div style="font-size: 14px; line-height: 1.6;">
                                <h2>📚 快速开始</h2>
                                
                                <h3>🔑 认证方式</h3>
                                <pre style="background: #f5f5f5; padding: 10px; border-radius: 4px;">
                                Authorization: Pufferfish-Bearer:eyJhbGciOiJIUzI1NiIs...
                                </pre>
                                
                                <h3>📊 状态码说明</h3>
                                <ul>
                                <li><span style="color: #28a745;">200</span> - 请求成功</li>
                                <li><span style="color: #007bff;">400</span> - 请求参数错误</li>
                                <li><span style="color: #ffc107;">401</span> - 未授权</li>
                                <li><span style="color: #dc3545;">500</span> - 服务器内部错误</li>
                                </ul>
                                
                                <h3>📝 使用限制</h3>
                                <ul>
                                <li>API调用频率限制：100次/分钟</li>
                                <li>请求超时时间：30秒</li>
                                </ul>
                                </div>
                                """)
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Mayc")
                                .email("203373485@qq.com")
                                .url("https://github.com/mayc0202"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("http://www.apache.org/licenses/LICENSE-2.0.html"))
                        .termsOfService("https://pufferfish-scheduler.com/terms"))
                .externalDocs(new ExternalDocumentation()
                        .description("项目文档")
                        .url("https://pufferfish-scheduler.com/docs"))
                .components(new io.swagger.v3.oas.models.Components()
                        .addSecuritySchemes(securitySchemeName, new SecurityScheme()
                                .name(securitySchemeName)
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")
                                .description("请输入JWT令牌")));
    }
}