package com.pufferfishscheduler.common.config.jwt;

import com.pufferfishscheduler.common.constants.Constants;
import io.jsonwebtoken.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Data
@Component
@ConfigurationProperties(prefix = "jwt")
public class JwtConfig {

    @Value("${jwt.token-secret}")
    private String secret;

    @Value("${jwt.expiration-time}")
    private Long expiration;

    @Value("${jwt.issuer}")
    private String issuer;

    private static final String TYPE = "JWT";

    private static final String ALG = "HS256";

    /**
     * 构建token
     *
     * @param userId
     * @param account
     * @return
     */
    public String buildToken(Integer userId, String account) {
        Map<String, Object> claims = new HashMap<>();
        claims.put(Constants.TOKEN_CONFIG.CLAIM_USER_ID, userId);
        claims.put(Constants.TOKEN_CONFIG.CLAIM_ACCOUNT, account);

        return Jwts.builder()
                .setHeaderParam("type", TYPE)
                .setHeaderParam("alg", ALG)
                .setIssuer(issuer)
                .setAudience("web")
                .setSubject(account)
                .setClaims(claims) // 使用setClaims而不是单独的claim方法
                .setExpiration(new Date(System.currentTimeMillis() + expiration))
                .setIssuedAt(new Date())
                .signWith(SignatureAlgorithm.HS256, secret)
                .compact();
    }

    /**
     * 解析token
     *
     * @param token
     * @return
     */
    public Jws<Claims> parseToken(String token) {
        return Jwts.parser()
                .setSigningKey(secret)
                .parseClaimsJws(token);
    }

    /**
     * 从token中获取用户ID
     */
    public Integer getUserIdFromToken(String token) {
        Claims claims = parseToken(token).getBody();
        return claims.get(Constants.TOKEN_CONFIG.CLAIM_USER_ID, Integer.class);
    }

    /**
     * 从token中获取账号
     */
    public String getAccountFromToken(String token) {
        Claims claims = parseToken(token).getBody();
        return claims.get(Constants.TOKEN_CONFIG.CLAIM_ACCOUNT, String.class);
    }

    /**
     * 基础Token校验
     *
     * @param token JWT令牌
     * @return 是否有效
     */
    public boolean verifyToken(String token) {
        if (StringUtils.isBlank(token)) {
            return false;
        }

        try {
            // 移除Bearer前缀
            if (token.startsWith(Constants.TOKEN_CONFIG.BEARER_PREFIX)) {
                token = token.replace(Constants.TOKEN_CONFIG.BEARER_PREFIX, "");
            }

            // 检查是否符合JWT格式
            String[] segments = token.split("\\.");
            if (segments.length != 3) {
                return false;
            }

            parseToken(token);
            return true;
        } catch (ExpiredJwtException e) {
            log.warn("Token已过期: {}", e.getMessage());
            return false;
        } catch (MalformedJwtException e) {
            log.warn("Token格式错误: {}", e.getMessage());
            return false;
        } catch (SignatureException e) {
            log.warn("签名验证失败: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            log.warn("Token验证异常: {}", e.getMessage());
            return false;
        }
    }
}
