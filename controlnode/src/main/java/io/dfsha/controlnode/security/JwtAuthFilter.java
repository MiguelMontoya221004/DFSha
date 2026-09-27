package io.dfsha.controlnode.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;

/**
 * Valida el JWT de cada petición y deja la identidad en el atributo "principal".
 * Rutas públicas: /api/v1/auth/**, /api/v1/health.
 */
@Component
public class JwtAuthFilter extends OncePerRequestFilter {

    private final JwtService jwt;
    private final ObjectMapper mapper = new ObjectMapper();

    public JwtAuthFilter(JwtService jwt) { this.jwt = jwt; }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String p = request.getRequestURI();
        return p.startsWith("/api/v1/auth/") || p.equals("/api/v1/health") || p.equals("/health");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String header = req.getHeader("Authorization");
        Principal principal = null;
        if (header != null && header.startsWith("Bearer ")) {
            principal = jwt.verify(header.substring(7).trim());
        }
        if (principal == null) {
            res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            res.setContentType("application/json");
            mapper.writeValue(res.getOutputStream(),
                    Map.of("error", "UNAUTHORIZED", "message", "Token ausente, inválido o expirado"));
            return;
        }
        req.setAttribute("principal", principal);
        chain.doFilter(req, res);
    }
}
