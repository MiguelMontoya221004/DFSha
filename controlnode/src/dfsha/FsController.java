package dfsha;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Gestion del espacio de nombres (RF1): ls, stat, mkdir, rmdir, rm, mv, chmod.
 * El cliente resuelve cd/pwd por su cuenta, aqui todas las rutas son absolutas.
 */
@RestController
@RequestMapping("/fs")
public class FsController {

    private final RaftService raft;

    public FsController(RaftService raft) {
        this.raft = raft;
    }

    @GetMapping("/ls")
    public Object ls(@RequestParam String path, HttpServletRequest req) {
        return raft.read("ls", quien(req, "path", path));
    }

    @GetMapping("/stat")
    public Object stat(@RequestParam String path, HttpServletRequest req) {
        return raft.read("stat", quien(req, "path", path));
    }

    @PostMapping("/mkdir")
    public Object mkdir(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        Map<String, Object> args = quien(req, "path", Db.str(body, "path"));
        args.put("perms", body.get("perms") == null ? 755 : Db.lng(body, "perms"));
        args.put("now", System.currentTimeMillis());
        return raft.write("mkdir", args);
    }

    @DeleteMapping("/rmdir")
    public Object rmdir(@RequestParam String path, HttpServletRequest req) {
        return raft.write("rmdir", quien(req, "path", path));
    }

    @DeleteMapping("/rm")
    public Object rm(@RequestParam String path, HttpServletRequest req) {
        return raft.write("rm", quien(req, "path", path));
    }

    @PostMapping("/mv")
    public Object mv(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        Map<String, Object> args = quien(req, "from", Db.str(body, "from"));
        args.put("to", Db.str(body, "to"));
        args.put("now", System.currentTimeMillis());
        return raft.write("mv", args);
    }

    @PostMapping("/chmod")
    public Object chmod(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        Map<String, Object> args = quien(req, "path", Db.str(body, "path"));
        args.put("perms", Db.lng(body, "perms"));
        args.put("now", System.currentTimeMillis());
        return raft.write("chmod", args);
    }

    /** Arma los argumentos con el usuario y el grupo que vienen del JWT. */
    static Map<String, Object> quien(HttpServletRequest req, String clave, Object valor) {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put(clave, valor);
        args.put("user", Auth.usuario(req));
        args.put("group", Auth.grupo(req));
        return args;
    }
}
