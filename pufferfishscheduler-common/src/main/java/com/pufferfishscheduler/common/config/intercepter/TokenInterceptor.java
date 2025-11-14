package com.pufferfishscheduler.common.config.intercepter;

import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.config.jwt.JwtConfig;
import com.pufferfishscheduler.common.config.properties.InterceptorProperties;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import io.jsonwebtoken.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

/**
 * token拦截器
 */
@Slf4j
@Component("tokenInterceptor")
@Order(1) // 确保拦截器顺序
public class TokenInterceptor implements HandlerInterceptor {

    @Autowired
    private InterceptorProperties interceptorProperties;

    @Autowired
    private JwtConfig jwtConfig;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!interceptorProperties.getEnabled()) {
            return true;
        }

        // 完全依赖WebInterceptorConfig的路径排除，这里不做额外检查
        final String requestURI = request.getRequestURI();

        try {
            String token = extractToken(request);
            validateToken(token);

            Claims claims = jwtConfig.parseToken(token).getBody();
            Integer userId = claims.get(Constants.TOKEN_CONFIG.CLAIM_USER_ID, Integer.class);
            String account = claims.get(Constants.TOKEN_CONFIG.CLAIM_ACCOUNT, String.class);

            validateTokenStatus(token, userId, account);
            setUserContext(userId, account);

            log.debug("Token验证通过 - 用户: {}[{}] 访问: {}", account, userId, requestURI);
            return true;
        } catch (BusinessException e) {
            log.warn("Token验证失败 - {}: {}", requestURI, e.getMessage());
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            throw new BusinessException(e.getMessage());
        } catch (Exception e) {
            log.error("Token拦截器异常 - {}: {}", requestURI, e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            throw new BusinessException("系统内部错误!");
        }
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
                                Object handler, Exception ex) {
        UserContext.clear();
    }

    private String extractToken(HttpServletRequest request) {
        String token = null;

        // 首先尝试从Authorization头获取
        token = request.getHeader(Constants.TOKEN_CONFIG.TOKEN);

        // 如果header中没有，尝试从cookie中获取
        if (StringUtils.isBlank(token)) {
            Cookie[] cookies = request.getCookies();
            if (cookies != null) {
                for (Cookie cookie : cookies) {
                    if (Constants.TOKEN_CONFIG.TOKEN.equals(cookie.getName())) {
                        token = cookie.getValue();
                        break;
                    }
                }
            }
        }

        if (StringUtils.isBlank(token)) {
            log.warn("请求头未携带Authorization信息!");
            throw new BusinessException("用户未登录!");
        }

        if (token.startsWith(Constants.TOKEN_CONFIG.BEARER_PREFIX)) {
            token = token.substring(Constants.TOKEN_CONFIG.BEARER_PREFIX.length());
        }

        return token;
    }

    private void validateToken(String token) {
        if (!jwtConfig.verifyToken(token)) {
            throw new BusinessException("令牌无效或已过期!");
        }
    }

    private void validateTokenStatus(String token, Integer userId, String account) {
        if (userId == null || StringUtils.isBlank(account)) {
            throw new BusinessException("令牌信息不完整!");
        }

        // 检查黑名单
        String blacklistKey = Constants.TOKEN_CONFIG.BLACKLIST_KEY_PREFIX + token;
        if (redisTemplate.hasKey(blacklistKey)) {
            throw new BusinessException("令牌已失效，请重新登录!");
        }

        // 检查单点登录
        String redisKey = Constants.TOKEN_CONFIG.SSO_KEY_PREFIX + userId;
        String currentToken = redisTemplate.opsForValue().get(redisKey);

        if (!token.equals(currentToken)) {
            try {
                TimeUnit.MILLISECONDS.sleep(100); // 防止暴力枚举
            } catch (InterruptedException ignored) {
            }
            throw new BusinessException("账号已在其他地方登录!");
        }
    }

    private void setUserContext(Integer userId, String account) {
        UserContext.setCurrentUserId(userId);
        UserContext.setCurrentAccount(account);
    }
}
