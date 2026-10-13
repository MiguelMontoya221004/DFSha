package dfsha;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Los metadatos del sistema de archivos (la "maquina de estados" de Raft).
 *
 * Todo lo que modifica el estado entra por apply(), que solo se llama desde
 * MetadataStateMachine cuando Raft ya replico el comando en la mayoria de los
 * nodos. Las lecturas entran por query().
 *
 * Regla importante: los comandos deben ser DETERMINISTAS. Los ids, las fechas
 * y la eleccion de DataNodes se calculan antes, en el controlador, y viajan
 * dentro del comando; aqui nunca se llama a System.currentTimeMillis() ni se
 * generan UUID.
 */
@Component
public class Db {

    private final JdbcTemplate jdbc;

    public Db(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ===================== despacho de comandos =====================

    @Transactional
    public Object apply(long term, long index, String op, Map<String, Object> a) {
        Object result = switch (op) {
            case "register_user" -> registerUser(a);
            case "mkdir"         -> mkdir(a);
            case "rmdir"         -> rmdir(a);
            case "rm"            -> rm(a);
            case "mv"            -> mv(a);
            case "chmod"         -> chmod(a);
            case "dn_register"   -> dnRegister(a);
            case "begin_write"   -> beginWrite(a);
            case "commit_write"  -> commitWrite(a);
            case "abort_write"   -> abortWrite(a);
            case "renew_write"   -> renewWrite(a);
            case "lock"          -> lock(a);
            case "unlock"        -> unlock(a);
            case "add_replica"   -> addReplica(a);
            case "del_replica"   -> delReplica(a);
            default -> throw ApiException.badRequest("comando desconocido: " + op);
        };
        jdbc.update("UPDATE applied SET term = ?, idx = ? WHERE id = 1", term, index);
        return result;
    }

    public Object query(String op, Map<String, Object> a) {
        return switch (op) {
            case "get_user"     -> getUser(str(a, "username"));
            case "ls"           -> ls(a);
            case "stat"         -> statOrFail(norm(str(a, "path")));
            case "upload"       -> uploadOrFail(str(a, "upload_id"));
            case "read"         -> read(a);
            case "datanodes"    -> datanodes();
            case "all_replicas" -> allReplicas();
            case "summary"      -> summary();
            default -> throw ApiException.badRequest("consulta desconocida: " + op);
        };
    }

    /** Ultimo comando de Raft que alcanzo a aplicarse en esta copia. */
    public long[] applied() {
        return jdbc.queryForObject("SELECT term, idx FROM applied WHERE id = 1",
                (rs, i) -> new long[]{rs.getLong("term"), rs.getLong("idx")});
    }

    // ===================== usuarios =====================

    private Object registerUser(Map<String, Object> a) {
        String user = str(a, "username");
        String group = str(a, "group");
        long now = lng(a, "now");
        if (getUser(user) != null) throw ApiException.conflict("el usuario ya existe");
        jdbc.update("INSERT INTO users (username, password_hash, group_name, created_at) VALUES (?,?,?,?)",
                user, str(a, "password_hash"), group, now);
        String home = "/home/" + user;
        if (stat(home) == null) createDir(home, user, group, 755, now);
        return Map.of("username", user, "home", home);
    }

    public Map<String, Object> getUser(String username) {
        List<Map<String, Object>> r = jdbc.query(
                "SELECT username, password_hash, group_name FROM users WHERE username = ?",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("username", rs.getString("username"));
                    m.put("password_hash", rs.getString("password_hash"));
                    m.put("group", rs.getString("group_name"));
                    return m;
                }, username);
        return r.isEmpty() ? null : r.get(0);
    }

    // ===================== namespace =====================

    private static final RowMapper<Map<String, Object>> FILE = (rs, i) -> {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("path", rs.getString("path"));
        m.put("name", rs.getString("name"));
        m.put("is_dir", rs.getBoolean("is_dir"));
        m.put("owner", rs.getString("owner"));
        m.put("group", rs.getString("group_name"));
        m.put("perms", rs.getInt("perms"));
        m.put("size", rs.getLong("size"));
        m.put("version", rs.getInt("version"));
        m.put("created_at", rs.getLong("created_at"));
        m.put("modified_at", rs.getLong("modified_at"));
        return m;
    };

    public Map<String, Object> stat(String path) {
        List<Map<String, Object>> r = jdbc.query("SELECT * FROM files WHERE path = ?", FILE, path);
        return r.isEmpty() ? null : r.get(0);
    }

    private Map<String, Object> statOrFail(String path) {
        Map<String, Object> f = stat(path);
        if (f == null) throw ApiException.notFound("no existe: " + path);
        return f;
    }

    private Object ls(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        Map<String, Object> dir = statOrFail(path);
        if (!(Boolean) dir.get("is_dir")) throw ApiException.badRequest("no es un directorio: " + path);
        requirePerm(dir, str(a, "user"), str(a, "group"), 'r');
        List<Map<String, Object>> hijos = jdbc.query(
                "SELECT * FROM files WHERE parent = ? AND path <> parent ORDER BY is_dir DESC, name", FILE, path);
        return Map.of("path", path, "entries", hijos);
    }

    private void createDir(String path, String owner, String group, int perms, long now) {
        jdbc.update("INSERT INTO files (path,parent,name,is_dir,owner,group_name,perms,size,version,created_at,modified_at)"
                        + " VALUES (?,?,?,TRUE,?,?,?,0,0,?,?)",
                path, parentOf(path), nameOf(path), owner, group, perms, now, now);
    }

    private Object mkdir(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        String user = str(a, "user"), group = str(a, "group");
        if (stat(path) != null) throw ApiException.conflict("ya existe: " + path);
        Map<String, Object> padre = statOrFail(parentOf(path));
        if (!(Boolean) padre.get("is_dir")) throw ApiException.badRequest("el padre no es un directorio");
        requirePerm(padre, user, group, 'w');
        createDir(path, user, group, (int) lng(a, "perms"), lng(a, "now"));
        return Map.of("path", path);
    }

    private Object rmdir(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        Map<String, Object> f = statOrFail(path);
        if (!(Boolean) f.get("is_dir")) throw ApiException.badRequest("no es un directorio");
        requirePerm(f, str(a, "user"), str(a, "group"), 'w');
        Integer hijos = jdbc.queryForObject("SELECT COUNT(*) FROM files WHERE parent = ? AND path <> parent",
                Integer.class, path);
        if (hijos != null && hijos > 0) throw ApiException.conflict("el directorio no esta vacio");
        jdbc.update("DELETE FROM files WHERE path = ?", path);
        return Map.of("path", path);
    }

    private Object rm(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        Map<String, Object> f = statOrFail(path);
        if ((Boolean) f.get("is_dir")) throw ApiException.badRequest("es un directorio, use rmdir");
        requirePerm(f, str(a, "user"), str(a, "group"), 'w');
        jdbc.update("DELETE FROM replicas WHERE block_id IN (SELECT block_id FROM blocks WHERE path = ?)", path);
        jdbc.update("DELETE FROM blocks WHERE path = ?", path);
        jdbc.update("DELETE FROM uploads WHERE path = ?", path);
        jdbc.update("DELETE FROM locks WHERE path = ?", path);
        jdbc.update("DELETE FROM files WHERE path = ?", path);
        return Map.of("path", path);
    }

    private Object mv(Map<String, Object> a) {
        String from = norm(str(a, "from")), to = norm(str(a, "to"));
        long now = lng(a, "now");
        Map<String, Object> f = statOrFail(from);
        requirePerm(f, str(a, "user"), str(a, "group"), 'w');
        if (stat(to) != null) throw ApiException.conflict("el destino ya existe: " + to);
        Map<String, Object> destino = statOrFail(parentOf(to));
        requirePerm(destino, str(a, "user"), str(a, "group"), 'w');
        if (to.startsWith(from + "/")) throw ApiException.badRequest("no se puede mover un directorio dentro de si mismo");

        List<String> rutas = jdbc.queryForList("SELECT path FROM files WHERE path = ? OR path LIKE ?",
                String.class, from, from + "/%");
        for (String vieja : rutas) {
            String nueva = to + vieja.substring(from.length());
            jdbc.update("UPDATE files SET path = ?, parent = ?, name = ?, modified_at = ? WHERE path = ?",
                    nueva, parentOf(nueva), nameOf(nueva), now, vieja);
            jdbc.update("UPDATE blocks SET path = ? WHERE path = ?", nueva, vieja);
        }
        return Map.of("from", from, "to", to, "movidos", rutas.size());
    }

    private Object chmod(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        Map<String, Object> f = statOrFail(path);
        String user = str(a, "user");
        if (!user.equals(f.get("owner")) && !user.equals("root"))
            throw ApiException.forbidden("solo el dueno puede cambiar los permisos");
        int perms = (int) lng(a, "perms");
        if (perms < 0 || perms > 777) throw ApiException.badRequest("permisos invalidos");
        jdbc.update("UPDATE files SET perms = ?, modified_at = ? WHERE path = ?", perms, lng(a, "now"), path);
        return Map.of("path", path, "perms", perms);
    }

    // ===================== escritura (copy-on-write) =====================

    private Object beginWrite(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        String user = str(a, "user"), group = str(a, "group");
        long now = lng(a, "now");
        Map<String, Object> f = stat(path);
        int version;
        if (f == null) {
            Map<String, Object> padre = statOrFail(parentOf(path));
            requirePerm(padre, user, group, 'w');
            jdbc.update("INSERT INTO files (path,parent,name,is_dir,owner,group_name,perms,size,version,created_at,modified_at)"
                            + " VALUES (?,?,?,FALSE,?,?,?,0,0,?,?)",
                    path, parentOf(path), nameOf(path), user, group, (int) lng(a, "perms"), now, now);
            version = 1;
        } else {
            if ((Boolean) f.get("is_dir")) throw ApiException.badRequest("es un directorio: " + path);
            requirePerm(f, user, group, 'w');
            version = (int) lng(f, "version") + 1;
        }
        tomarLock(path, user, now, lng(a, "lease_until"));
        jdbc.update("INSERT INTO uploads (upload_id, path, version, owner, started_at) VALUES (?,?,?,?,?)",
                str(a, "upload_id"), path, version, user, now);
        return Map.of("upload_id", str(a, "upload_id"), "path", path, "version", version);
    }

    @SuppressWarnings("unchecked")
    private Object commitWrite(Map<String, Object> a) {
        String uploadId = str(a, "upload_id");
        Map<String, Object> up = upload(uploadId);
        if (up == null) throw ApiException.notFound("no hay una escritura abierta con ese id");
        if (!str(a, "user").equals(up.get("owner"))) throw ApiException.forbidden("esa escritura es de otro usuario");

        String path = (String) up.get("path");
        // El lease tiene que seguir siendo mio. Si se vencio y otro usuario
        // abrio el archivo, esta escritura ya no se puede publicar: si no se
        // revisara, los dos escribirian la misma version y el archivo quedaria
        // con los bloques de ambos mezclados.
        requiereLease(path, str(a, "user"), lng(a, "now"));

        // La version se decide AQUI y no en el begin_write, para que siempre
        // sea la siguiente a la que esta publicada en este momento.
        int version = (int) lng(statOrFail(path), "version") + 1;

        List<Map<String, Object>> bloques = (List<Map<String, Object>>) a.get("blocks");
        if (bloques == null) throw ApiException.badRequest("faltan los bloques");

        long size = 0;
        for (Map<String, Object> b : bloques) {
            jdbc.update("INSERT INTO blocks (block_id, path, version, idx, size, checksum) VALUES (?,?,?,?,?,?)",
                    str(b, "block_id"), path, version, (int) lng(b, "index"), lng(b, "size"), str(b, "checksum"));
            for (String nodo : (List<String>) b.get("nodes")) {
                jdbc.update("INSERT INTO replicas (block_id, node_id) VALUES (?,?)", str(b, "block_id"), nodo);
            }
            size += lng(b, "size");
        }
        // la version nueva se publica aqui, de forma atomica
        jdbc.update("UPDATE files SET version = ?, size = ?, modified_at = ? WHERE path = ?",
                version, size, lng(a, "now"), path);
        jdbc.update("DELETE FROM uploads WHERE upload_id = ?", uploadId);
        jdbc.update("DELETE FROM locks WHERE path = ?", path);
        return Map.of("path", path, "version", version, "size", size, "blocks", bloques.size());
    }

    private Object abortWrite(Map<String, Object> a) {
        Map<String, Object> up = upload(str(a, "upload_id"));
        if (up == null) throw ApiException.notFound("no hay una escritura abierta con ese id");
        jdbc.update("DELETE FROM uploads WHERE upload_id = ?", str(a, "upload_id"));
        // solo se suelta el lock si todavia es mio: si ya se vencio y lo tomo
        // otro usuario, no se le puede quitar
        jdbc.update("DELETE FROM locks WHERE path = ? AND owner = ?", up.get("path"), up.get("owner"));
        return Map.of("upload_id", str(a, "upload_id"), "estado", "cancelada");
    }

    /** Estira el lease de una escritura que va lenta (archivos grandes). */
    private Object renewWrite(Map<String, Object> a) {
        String uploadId = str(a, "upload_id");
        Map<String, Object> up = upload(uploadId);
        if (up == null) throw ApiException.notFound("no hay una escritura abierta con ese id");
        if (!str(a, "user").equals(up.get("owner"))) throw ApiException.forbidden("esa escritura es de otro usuario");
        String path = (String) up.get("path");
        requiereLease(path, str(a, "user"), lng(a, "now"));
        jdbc.update("UPDATE locks SET expires_at = ? WHERE path = ?", lng(a, "lease_until"), path);
        return Map.of("upload_id", uploadId, "path", path, "lease_hasta", lng(a, "lease_until"));
    }

    private Map<String, Object> uploadOrFail(String uploadId) {
        Map<String, Object> up = upload(uploadId);
        if (up == null) throw ApiException.notFound("no hay una escritura abierta con ese id");
        // se agrega el estado del lease para que el controlador no entregue
        // tokens de bloque de una escritura que ya se vencio
        Map<String, Object> l = lockDe((String) up.get("path"));
        up.put("lease_owner", l == null ? "" : l.get("owner"));
        up.put("lease_hasta", l == null ? 0L : lng(l, "expires_at"));
        return up;
    }

    public Map<String, Object> upload(String uploadId) {
        List<Map<String, Object>> r = jdbc.query("SELECT * FROM uploads WHERE upload_id = ?",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("upload_id", rs.getString("upload_id"));
                    m.put("path", rs.getString("path"));
                    m.put("version", rs.getInt("version"));
                    m.put("owner", rs.getString("owner"));
                    return m;
                }, uploadId);
        return r.isEmpty() ? null : r.get(0);
    }

    // ===================== lectura =====================

    private Object read(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        Map<String, Object> f = statOrFail(path);
        if ((Boolean) f.get("is_dir")) throw ApiException.badRequest("es un directorio: " + path);
        requirePerm(f, str(a, "user"), str(a, "group"), 'r');
        int version = a.get("version") == null ? (int) lng(f, "version") : (int) lng(a, "version");
        if (version == 0) throw ApiException.notFound("el archivo todavia no tiene ninguna version publicada");

        List<Map<String, Object>> bloques = jdbc.query(
                "SELECT block_id, idx, size, checksum FROM blocks WHERE path = ? AND version = ? ORDER BY idx",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("block_id", rs.getString("block_id"));
                    m.put("index", rs.getInt("idx"));
                    m.put("size", rs.getLong("size"));
                    m.put("checksum", rs.getString("checksum"));
                    return m;
                }, path, version);
        for (Map<String, Object> b : bloques) {
            b.put("nodes", nodesOf((String) b.get("block_id")));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("path", path);
        out.put("version", version);
        out.put("size", f.get("size"));
        out.put("blocks", bloques);
        return out;
    }

    public List<String> nodesOf(String blockId) {
        return jdbc.queryForList("SELECT node_id FROM replicas WHERE block_id = ?", String.class, blockId);
    }

    // ===================== locks (lease) =====================

    /** Falla si el lease del archivo ya no es de este usuario o ya se vencio. */
    private void requiereLease(String path, String user, long now) {
        Map<String, Object> l = lockDe(path);
        if (l == null || !user.equals(l.get("owner")) || lng(l, "expires_at") <= now) {
            throw ApiException.conflict("se vencio el lease de " + path
                    + "; renuevelo con /files/write/renew mientras sube, o empiece la escritura de nuevo");
        }
    }

    private Map<String, Object> lockDe(String path) {
        List<Map<String, Object>> r = jdbc.query("SELECT owner, expires_at FROM locks WHERE path = ?",
                (rs, i) -> Map.of("owner", rs.getString("owner"), "expires_at", rs.getLong("expires_at")), path);
        return r.isEmpty() ? null : r.get(0);
    }

    private void tomarLock(String path, String user, long now, long hasta) {
        List<Map<String, Object>> r = jdbc.query("SELECT owner, expires_at FROM locks WHERE path = ?",
                (rs, i) -> Map.of("owner", rs.getString("owner"), "expires_at", rs.getLong("expires_at")), path);
        if (!r.isEmpty()) {
            Map<String, Object> l = r.get(0);
            if (lng(l, "expires_at") > now && !user.equals(l.get("owner")))
                throw ApiException.conflict("el archivo esta bloqueado por " + l.get("owner"));
            jdbc.update("UPDATE locks SET owner = ?, expires_at = ? WHERE path = ?", user, hasta, path);
        } else {
            jdbc.update("INSERT INTO locks (path, owner, expires_at) VALUES (?,?,?)", path, user, hasta);
        }
    }

    private Object lock(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        statOrFail(path);
        tomarLock(path, str(a, "user"), lng(a, "now"), lng(a, "lease_until"));
        return Map.of("path", path, "owner", str(a, "user"), "expires_at", lng(a, "lease_until"));
    }

    private Object unlock(Map<String, Object> a) {
        String path = norm(str(a, "path"));
        int n = jdbc.update("DELETE FROM locks WHERE path = ? AND owner = ?", path, str(a, "user"));
        if (n == 0) throw ApiException.forbidden("usted no tiene el lock de " + path);
        return Map.of("path", path, "estado", "liberado");
    }

    // ===================== DataNodes y replicas =====================

    private Object dnRegister(Map<String, Object> a) {
        jdbc.update("MERGE INTO datanodes (node_id, host, port, zone, capacity, registered_at)"
                        + " KEY(node_id) VALUES (?,?,?,?,?,?)",
                str(a, "node_id"), str(a, "host"), (int) lng(a, "port"), str(a, "zone"),
                lng(a, "capacity"), lng(a, "now"));
        return Map.of("node_id", str(a, "node_id"));
    }

    private List<Map<String, Object>> datanodes() {
        return jdbc.query("SELECT * FROM datanodes ORDER BY node_id", (rs, i) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("node_id", rs.getString("node_id"));
            m.put("host", rs.getString("host"));
            m.put("port", rs.getInt("port"));
            m.put("zone", rs.getString("zone"));
            m.put("capacity", rs.getLong("capacity"));
            return m;
        });
    }

    private Object addReplica(Map<String, Object> a) {
        jdbc.update("MERGE INTO replicas (block_id, node_id) KEY(block_id, node_id) VALUES (?,?)",
                str(a, "block_id"), str(a, "node_id"));
        return Map.of("block_id", str(a, "block_id"), "node_id", str(a, "node_id"));
    }

    private Object delReplica(Map<String, Object> a) {
        jdbc.update("DELETE FROM replicas WHERE block_id = ? AND node_id = ?",
                str(a, "block_id"), str(a, "node_id"));
        return Map.of("block_id", str(a, "block_id"), "node_id", str(a, "node_id"));
    }

    /** Todos los bloques con la lista de DataNodes que los tienen. Lo usa el
     *  chequeo de bloques sub-replicados. */
    private List<Map<String, Object>> allReplicas() {
        List<Map<String, Object>> bloques = jdbc.query("SELECT block_id, path, size FROM blocks", (rs, i) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("block_id", rs.getString("block_id"));
            m.put("path", rs.getString("path"));
            m.put("size", rs.getLong("size"));
            return m;
        });
        for (Map<String, Object> b : bloques) b.put("nodes", nodesOf((String) b.get("block_id")));
        return bloques;
    }

    private Object summary() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("usuarios", count("users"));
        m.put("directorios", jdbc.queryForObject("SELECT COUNT(*) FROM files WHERE is_dir = TRUE", Integer.class));
        m.put("archivos", jdbc.queryForObject("SELECT COUNT(*) FROM files WHERE is_dir = FALSE", Integer.class));
        m.put("bloques", count("blocks"));
        m.put("replicas", count("replicas"));
        m.put("datanodes_registrados", count("datanodes"));
        m.put("escrituras_abiertas", count("uploads"));
        return m;
    }

    private int count(String tabla) {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM " + tabla, Integer.class);
        return n == null ? 0 : n;
    }

    // ===================== permisos y rutas =====================

    private void requirePerm(Map<String, Object> f, String user, String group, char necesita) {
        if ("root".equals(user)) return;
        int perms = (int) lng(f, "perms");
        int digito;
        if (user.equals(f.get("owner"))) digito = perms / 100;
        else if (group.equals(f.get("group"))) digito = (perms / 10) % 10;
        else digito = perms % 10;
        int bit = switch (necesita) {
            case 'r' -> 4;
            case 'w' -> 2;
            default -> 1;
        };
        if ((digito & bit) == 0)
            throw ApiException.forbidden("sin permiso '" + necesita + "' sobre " + f.get("path"));
    }

    public static String norm(String p) {
        if (p == null || p.isBlank()) throw ApiException.badRequest("ruta vacia");
        String s = p.trim().replace('\\', '/');
        while (s.contains("//")) s = s.replace("//", "/");
        if (!s.startsWith("/")) throw ApiException.badRequest("la ruta debe ser absoluta: " + p);
        if (s.length() > 1 && s.endsWith("/")) s = s.substring(0, s.length() - 1);
        for (String parte : s.split("/")) {
            if (parte.equals("..")) throw ApiException.badRequest("ruta invalida: " + p);
        }
        return s;
    }

    public static String parentOf(String path) {
        int i = path.lastIndexOf('/');
        return i <= 0 ? "/" : path.substring(0, i);
    }

    public static String nameOf(String path) {
        int i = path.lastIndexOf('/');
        return (i < 0 || path.equals("/")) ? path : path.substring(i + 1);
    }

    // ===================== helpers para leer el comando =====================

    static String str(Map<String, Object> a, String k) {
        Object v = a.get(k);
        if (v == null) throw ApiException.badRequest("falta el campo '" + k + "'");
        return v.toString();
    }

    static long lng(Map<String, Object> a, String k) {
        Object v = a.get(k);
        if (v == null) throw ApiException.badRequest("falta el campo '" + k + "'");
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString());
    }
}
