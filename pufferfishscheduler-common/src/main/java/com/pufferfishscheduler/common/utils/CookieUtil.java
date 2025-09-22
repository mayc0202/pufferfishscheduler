package com.pufferfishscheduler.common.utils;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.util.Objects;

/**
 * Cookie工具类
 */
public class CookieUtil {

    /**
     * 获取token
     *
     * @return
     */
    public static String getToken() {

        HttpServletRequest request = ((ServletRequestAttributes) Objects.requireNonNull(RequestContextHolder.getRequestAttributes())).getRequest();
        // 更健壮的 Cookie 处理
        if (request.getCookies() == null) {
            throw new BusinessException("请求中未包含任何 Cookie");
        }

        Cookie[] cookies = request.getCookies();
        if (Objects.isNull(cookies)) {
            throw new BusinessException("请校验Cookie中token是否为空!");
        }

        String token = "";
        for (Cookie cookie : cookies) {
            if (Constants.TOKEN_CONFIG.TOKEN.equals(cookie.getName())) {
                token = cookie.getValue().replaceAll(Constants.TOKEN_CONFIG.BEARER_PREFIX, "");
                break;
            }
        }

        if (StringUtils.isBlank(token)) {
            throw new BusinessException("请校验token是否为空!");
        }

        return token;
    }
}
