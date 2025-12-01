package com.pufferfishscheduler.service.upms.impl;

import com.pufferfishscheduler.common.config.jwt.JwtConfig;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.CookieUtil;
import com.pufferfishscheduler.common.utils.RSAUtil;
import com.pufferfishscheduler.domain.vo.user.AuthVo;
import com.pufferfishscheduler.domain.vo.user.RoleVo;
import com.pufferfishscheduler.domain.vo.user.UserVo;
import com.pufferfishscheduler.dao.entity.User;
import com.pufferfishscheduler.domain.form.auth.LoginForm;
import com.pufferfishscheduler.service.upms.AuthService;
import com.pufferfishscheduler.service.upms.RoleService;
import com.pufferfishscheduler.service.upms.UserService;
import io.jsonwebtoken.Claims;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 * @author Mayc
 * @since 2025-09-21  18:53
 */
@Slf4j
@Service
public class AuthServiceImpl implements AuthService {

    @Value("${jwt.expiration-time}")
    private Long expiration;

    @Autowired
    private AESUtil aesUtil;

    @Autowired
    private RSAUtil rsaUtil;

    @Autowired
    private UserService userService;

    @Autowired
    private RoleService roleService;

    @Autowired
    private JwtConfig jwtConfig;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;


    /**
     * 获取认证信息: RSA公钥
     *
     * @return
     */
    @Override
    public AuthVo getAuth() {
        AuthVo auth = new AuthVo();
        auth.setPublicKey(rsaUtil.getPublicKeyStr());
        return auth;
    }

    /**
     * 用户登录
     *
     * @param loginForm
     * @return
     */
    @Override
    public String login(LoginForm loginForm) {
        // 1. 获取用户信息
        User user = userService.getOneUserByIdAndAccount(null, loginForm.getUsername());
        if (user == null) {
            log.warn("用户[{}]不存在!", loginForm.getUsername());
            throw new BusinessException("用户名或密码错误!");
        }

        // 2. 密码校验
        try {
            String aesPwd = aesUtil.decrypt(user.getPassword());
            String rsaPwd = rsaUtil.decrypt(loginForm.getPassword());

            if (!aesPwd.equals(rsaPwd)) {
                log.warn("用户[{}]密码不正确!", loginForm.getUsername());
                throw new BusinessException("用户名或密码错误!");
            }
        } catch (Exception e) {
            log.error("密码解密失败: {}", e.getMessage(), e);
            throw new BusinessException("登录处理失败，请稍后重试!");
        }

        // 3. 创建token
        String token = jwtConfig.buildToken(user.getId(), user.getAccount());

        // 4. 记录登录状态
        afterLoginSuccess(user.getId(), token);

        // 5. 返回带Bearer前缀的token
        return Constants.TOKEN_CONFIG.BEARER_PREFIX + token;
    }

    /**
     * 用户注销
     */
    @Override
    public void logout() {
        String token = CookieUtil.getToken();
        if (StringUtils.isBlank(token)) {
            return;
        }

        try {
            Integer userId = jwtConfig.getUserIdFromToken(token);
            if (userId != null) {
                // 加入黑名单
                String blacklistKey = Constants.TOKEN_CONFIG.BLACKLIST_KEY_PREFIX + token;
                Claims claims = jwtConfig.parseToken(token).getBody();
                long ttl = claims.getExpiration().getTime() - System.currentTimeMillis();

                if (ttl > 0) {
                    redisTemplate.opsForValue().set(
                            blacklistKey,
                            "1",
                            ttl,
                            TimeUnit.MILLISECONDS
                    );
                }

                // 删除Redis中的Token记录
                redisTemplate.delete(Constants.TOKEN_CONFIG.SSO_KEY_PREFIX + userId);
            }
        } catch (Exception e) {
            log.error("注销时处理token异常: {}", e.getMessage());
        }
    }

    /**
     * 获取用户信息
     *
     * @param token
     * @return
     */
    @Override
    public UserVo getUserInfo(String token) {

        if (StringUtils.isBlank(token)) {
            token = CookieUtil.getToken();
        }

        try {
            Integer userId = jwtConfig.getUserIdFromToken(token);
            String account = jwtConfig.getAccountFromToken(token);

            if (userId == null || StringUtils.isBlank(account)) {
                throw new BusinessException("令牌信息不完整!");
            }

            User user = userService.getOneUserByIdAndAccount(userId, account);
            if (user == null) {
                throw new BusinessException("请校验用户信息是否存在!");
            }

            // 查询用户角色
            List<RoleVo> roleVos = roleService.getRoleListByUserId(userId);
            if (roleVos.isEmpty()) {
                throw new BusinessException("请验证用户角色是否为空!");
            }
            Set<String> roleList = roleVos.stream().map(RoleVo::getRoleName).collect(Collectors.toSet());

            // 复制
            UserVo userVo = new UserVo();
            BeanUtils.copyProperties(user, userVo);
            userVo.setRoles(roleList);

            return userVo;
        } catch (Exception e) {
            log.error("获取用户信息失败: {}", e.getMessage(), e);
            throw new BusinessException("获取用户信息失败!");
        }
    }

    /**
     * 登录成功后置操作
     *
     * @param userId
     * @param newToken
     */
    public void afterLoginSuccess(Integer userId, String newToken) {
        String redisKey = Constants.TOKEN_CONFIG.SSO_KEY_PREFIX + userId;

        try {
            // 1. 获取旧令牌并加入黑名单
            String oldToken = redisTemplate.opsForValue().get(redisKey);
            if (StringUtils.isNotBlank(oldToken)) {
                try {
                    Claims oldClaims = jwtConfig.parseToken(oldToken).getBody();
                    long ttl = oldClaims.getExpiration().getTime() - System.currentTimeMillis();

                    if (ttl > 0) {
                        String blacklistKey = Constants.TOKEN_CONFIG.BLACKLIST_KEY_PREFIX + oldToken;
                        redisTemplate.opsForValue().set(
                                blacklistKey,
                                "1",
                                ttl,
                                TimeUnit.MILLISECONDS
                        );
                    }
                } catch (Exception e) {
                    log.warn("旧Token解析失败: {}", oldToken, e);
                    // 旧token无效，继续处理
                }
            }

            // 2. 存储新令牌
            redisTemplate.opsForValue().set(
                    redisKey,
                    newToken,
                    jwtConfig.getExpiration(),
                    TimeUnit.MILLISECONDS
            );

        } catch (Exception e) {
            log.error("登录后处理Redis异常: {}", e.getMessage(), e);
            throw new BusinessException("系统繁忙，请稍后重试!");
        }
    }

    /**
     * 刷新token
     *
     * @param oldToken
     * @return
     */
    @Override
    public String refreshToken(String oldToken) {
        try {
            Claims claims = jwtConfig.parseToken(oldToken).getBody();

            // 剩余时间小于30分钟才刷新
            if (claims.getExpiration().getTime() - System.currentTimeMillis() > expiration) {
                return oldToken;
            }

            Integer userId = claims.get(Constants.TOKEN_CONFIG.CLAIM_USER_ID, Integer.class);
            String account = claims.get(Constants.TOKEN_CONFIG.CLAIM_ACCOUNT, String.class);

            if (userId == null || StringUtils.isBlank(account)) {
                throw new BusinessException("令牌信息不完整，无法刷新!");
            }

            String newToken = jwtConfig.buildToken(userId, account);
            afterLoginSuccess(userId, newToken);

            return Constants.TOKEN_CONFIG.BEARER_PREFIX + newToken;
        } catch (Exception e) {
            log.error("刷新Token失败: {}", e.getMessage(), e);
            throw new BusinessException("Token刷新失败!");
        }
    }
}
