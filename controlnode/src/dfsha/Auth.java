package dfsha;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;

/**
 * Seguridad del ControlNode:
 *
 *  - contrasenas con bcrypt
 *  - sesiones con JWT (el cliente manda "Authorization: Bearer ...")
 *  - tokens de bloque firmados con HMAC: el ControlNode autoriza y el DataNode
 *    verifica la firma sin tener que preguntarle nada al ControlNode.
 *
 * Esta misma clase es el filtro que valida el JWT en cada peticion.
 */
@Component
public class Auth extends OncePerRequestFilter {

    /** Rutas que no piden JWT. */
    private static final String[] PUBLICAS = {
            "/auth/login", "/auth/register", "/health", "/cluster", "/datanodes/register", "/datanodes/heartbeat", "/datanodes/block-received"
    };

    private final BCryptPasswordEncoder bcrypt = new BCryptPasswordEncoder();

    @Value("${dfsha.jwt-secret}")
    private String jwtSecret;
    @Value("${dfsha.jwt-minutes}")
    private long jwtMinutos;
    @Value("${dfsha.block-secret}")
    private String blockSecret;

    private SecretKey jwtKey;

    @PostConstruct
    void init() {
        jwtKey = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
    }

    // ===================== contrasenas =====================

    public String hash(String password) {
        return bcrypt.encode(password);
    }

    public boolean passwordCorrecta(String password, String hash) {
        return bcrypt.matches(password, hash);
    }

    // ===================== JWT =====================

    public String nuevoJwt(String usuario, String grupo) {
        Date ahora = new Date();
        return Jwts.builder()
                .subject(usuario)
                .claim("grupo", grupo)
                .issuedAt(ahora)
                .expiration(new Date(ahora.getTime() + jwtMinutos * 60_000))
                .signWith(jwtKey)
                .compact();
    }

    public long duracionJwtSegundos() {
        return jwtMinutos * 60;
    }

    private Claims validarJwt(String token) {
        return Jwts.parser().verifyWith(jwtKey).build().parseSignedClaims(token).getPayload();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String ruta = req.getRequestURI();
        boolean publica = false;
        for (String p : PUBLICAS) if (ruta.startsWith(p)) publica = true;

        String cabecera = req.getHeader("Authorization");
        if (cabecera != null && cabecera.startsWith("Bearer ")) {
            try {
                Claims c = validarJwt(cabecera.substring(7));
                req.setAttribute("usuario", c.getSubject());
                req.setAttribute("grupo", c.get("grupo", String.class));
            } catch (Exception e) {
                if (!publica) {
                    responder(res, 401, "token invalido o vencido");
                    return;
                }
            }
        } else if (!publica) {
            responder(res, 401, "falta el encabezado Authorization: Bearer <jwt>");
            return;
        }
        chain.doFilter(req, res);
    }

    private void responder(HttpServletResponse res, int status, String mensaje) throws IOException {
        res.setStatus(status);
        res.setContentType("application/json;charset=UTF-8");
        res.getWriter().write("{\"error\":\"" + mensaje + "\"}");
    }

    /** Usuario autenticado de la peticion actual. */
    public static String usuario(HttpServletRequest req) {
        Object u = req.getAttribute("usuario");
        if (u == null) throw ApiException.unauthorized("no autenticado");
        return u.toString();
    }

    public static String grupo(HttpServletRequest req) {
        Object g = req.getAttribute("grupo");
        return g == null ? "users" : g.toString();
    }

    // ===================== tokens de bloque =====================

    /**
     * Permiso para leer o escribir UN bloque en un DataNode.
     * Formato:  <blockId>:<usuario>:<op>:<vence>:<firma>
     * El DataNode recalcula la firma con el mismo secreto compartido.
     */
    public String tokenDeBloque(String blockId, String usuario, String op, long venceEnMs) {
        String cuerpo = blockId + ":" + usuario + ":" + op + ":" + venceEnMs;
        return cuerpo + ":" + firmar(cuerpo);
    }

    private String firmar(String texto) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(blockSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(mac.doFinal(texto.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("no se pudo firmar el token de bloque", e);
        }
    }
}
