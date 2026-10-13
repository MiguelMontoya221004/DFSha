package dfsha;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/** Registro y login de usuarios (RF4). */
@RestController
@RequestMapping("/auth")
public class AuthController {

    private final RaftService raft;
    private final Auth auth;

    public AuthController(RaftService raft, Auth auth) {
        this.raft = raft;
        this.auth = auth;
    }

    @PostMapping("/register")
    public Object register(@RequestBody Map<String, Object> body) {
        String usuario = Db.str(body, "username");
        String password = Db.str(body, "password");
        String grupo = body.get("group") == null ? "users" : body.get("group").toString();

        if (!usuario.matches("[a-zA-Z0-9_.-]{3,32}"))
            throw ApiException.badRequest("el usuario debe tener entre 3 y 32 letras, numeros, '.', '_' o '-'");
        if (usuario.equals("root"))
            throw ApiException.forbidden("el usuario 'root' esta reservado");
        if (password.length() < 6)
            throw ApiException.badRequest("la contrasena debe tener al menos 6 caracteres");

        Map<String, Object> args = new LinkedHashMap<>();
        args.put("username", usuario);
        args.put("password_hash", auth.hash(password));
        args.put("group", grupo);
        args.put("now", System.currentTimeMillis());
        return raft.write("register_user", args);
    }

    @PostMapping("/login")
    @SuppressWarnings("unchecked")
    public Object login(@RequestBody Map<String, Object> body) {
        String usuario = Db.str(body, "username");
        String password = Db.str(body, "password");

        Map<String, Object> u = (Map<String, Object>) raft.read("get_user", Map.of("username", usuario));
        if (u == null || !auth.passwordCorrecta(password, (String) u.get("password_hash")))
            throw ApiException.unauthorized("usuario o contrasena incorrectos");

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("token", auth.nuevoJwt(usuario, (String) u.get("group")));
        r.put("expira_en_segundos", auth.duracionJwtSegundos());
        r.put("usuario", usuario);
        r.put("grupo", u.get("group"));
        r.put("home", "/home/" + usuario);
        return r;
    }
}
