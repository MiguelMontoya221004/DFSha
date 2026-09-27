package io.dfsha.controlnode.security;

import io.dfsha.controlnode.config.ControlNodeProperties;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;

/** Emite y verifica los JWT de sesión (HS256). */
@Service
public class JwtService {

    private final SecretKey key;
    private final long ttlMinutes;

    public JwtService(ControlNodeProperties props) {
        this.key = Keys.hmacShaKeyFor(props.jwtSecret().getBytes(StandardCharsets.UTF_8));
        this.ttlMinutes = props.jwtTtlMinutes();
    }

    public String issue(String username, String role) {
        Instant now = Instant.now();
        return Jwts.builder()
                .subject(username)
                .claim("role", role)
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plusSeconds(ttlMinutes * 60)))
                .signWith(key)
                .compact();
    }

    public long ttlSeconds() { return ttlMinutes * 60; }

    /** @return la identidad del token, o null si es inválido o expiró. */
    public Principal verify(String token) {
        try {
            Claims c = Jwts.parser().verifyWith(key).build()
                    .parseSignedClaims(token).getPayload();
            return new Principal(c.getSubject(), String.valueOf(c.get("role")));
        } catch (JwtException | IllegalArgumentException e) {
            return null;
        }
    }
}
