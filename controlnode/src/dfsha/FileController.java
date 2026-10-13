package dfsha;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Transferencia y acceso a archivos (RF2 y RF3).
 *
 * Por aqui NO pasan los bytes. El flujo de escritura es:
 *
 *   1. POST /files/write/begin     -> abre una version nueva y toma el lease
 *   2. POST /files/write/allocate  -> por cada bloque: id + los 3 DataNodes + token
 *      (el cliente le manda el bloque al primero y ese lo replica en pipeline)
 *   3. POST /files/write/commit    -> publica la version completa de una sola vez
 *
 * Y el de lectura:
 *
 *   GET /files/read?path=...  -> lista de bloques, donde esta cada uno y su token
 */
@RestController
@RequestMapping("/files")
public class FileController {

    private final RaftService raft;
    private final DataNodes datanodes;
    private final Auth auth;

    @Value("${dfsha.block-size}")
    private long blockSize;
    @Value("${dfsha.replication}")
    private int replicacion;
    @Value("${dfsha.lease-seconds}")
    private long leaseSegundos;

    public FileController(RaftService raft, DataNodes datanodes, Auth auth) {
        this.raft = raft;
        this.datanodes = datanodes;
        this.auth = auth;
    }

    // ===================== escritura =====================

    @PostMapping("/write/begin")
    @SuppressWarnings("unchecked")
    public Object begin(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        long ahora = System.currentTimeMillis();
        long lease = ahora + leaseSegundos * 1000;

        Map<String, Object> args = FsController.quien(req, "path", Db.str(body, "path"));
        args.put("upload_id", UUID.randomUUID().toString());
        args.put("perms", body.get("perms") == null ? 644 : Db.lng(body, "perms"));
        args.put("now", ahora);
        args.put("lease_until", lease);

        Map<String, Object> r = new LinkedHashMap<>((Map<String, Object>) raft.write("begin_write", args));
        r.put("block_size", blockSize);
        r.put("replicas_por_bloque", replicacion);
        r.put("lease_hasta", lease);
        return r;
    }

    /** Reserva un bloque y dice en que DataNodes se debe escribir. */
    @PostMapping("/write/allocate")
    @SuppressWarnings("unchecked")
    public Object allocate(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        String uploadId = Db.str(body, "upload_id");
        String usuario = Auth.usuario(req);

        Map<String, Object> up = (Map<String, Object>) raft.read("upload", Map.of("upload_id", uploadId));
        if (!usuario.equals(up.get("owner")))
            throw ApiException.forbidden("esa escritura la abrio otro usuario");
        // sin lease vigente no se entregan tokens de escritura
        if (!usuario.equals(up.get("lease_owner")) || Db.lng(up, "lease_hasta") <= System.currentTimeMillis())
            throw ApiException.conflict("se vencio el lease; renuevelo con /files/write/renew o empiece de nuevo");

        String blockId = UUID.randomUUID().toString();
        List<DataNodes.Nodo> destinos = datanodes.elegirReplicas(replicacion, Set.of());

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("block_id", blockId);
        r.put("nodes", ubicaciones(destinos));
        r.put("token", auth.tokenDeBloque(blockId, usuario, "write", System.currentTimeMillis() + leaseSegundos * 1000));
        return r;
    }

    /** Publica la version nueva. Es atomico: o quedan todos los bloques o ninguno. */
    @PostMapping("/write/commit")
    public Object commit(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        Map<String, Object> args = FsController.quien(req, "upload_id", Db.str(body, "upload_id"));
        if (body.get("blocks") == null) throw ApiException.badRequest("faltan los bloques");
        args.put("blocks", body.get("blocks"));
        args.put("now", System.currentTimeMillis());
        return raft.write("commit_write", args);
    }

    /** Para archivos grandes: estira el lease mientras se siguen subiendo bloques. */
    @PostMapping("/write/renew")
    public Object renew(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        long ahora = System.currentTimeMillis();
        Map<String, Object> args = FsController.quien(req, "upload_id", Db.str(body, "upload_id"));
        args.put("now", ahora);
        args.put("lease_until", ahora + leaseSegundos * 1000);
        return raft.write("renew_write", args);
    }

    @PostMapping("/write/abort")
    public Object abort(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        return raft.write("abort_write", FsController.quien(req, "upload_id", Db.str(body, "upload_id")));
    }

    // ===================== lectura =====================

    @GetMapping("/read")
    @SuppressWarnings("unchecked")
    public Object read(@RequestParam String path,
                       @RequestParam(required = false) Integer version,
                       HttpServletRequest req) {
        String usuario = Auth.usuario(req);
        Map<String, Object> args = FsController.quien(req, "path", path);
        if (version != null) args.put("version", version);

        Map<String, Object> data = (Map<String, Object>) raft.read("read", args);
        long vence = System.currentTimeMillis() + leaseSegundos * 1000;

        for (Map<String, Object> b : (List<Map<String, Object>>) data.get("blocks")) {
            List<DataNodes.Nodo> vivos = new ArrayList<>();
            for (String id : (List<String>) b.get("nodes")) {
                DataNodes.Nodo n = datanodes.get(id);
                if (n != null && datanodes.vivo(id)) vivos.add(n);
            }
            if (vivos.isEmpty())
                throw ApiException.unavailable("ningun DataNode vivo tiene el bloque " + b.get("block_id"));
            b.put("nodes", ubicaciones(vivos));
            b.put("token", auth.tokenDeBloque((String) b.get("block_id"), usuario, "read", vence));
        }
        return data;
    }

    // ===================== locks (RF3) =====================

    @PostMapping("/lock")
    public Object lock(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        long ahora = System.currentTimeMillis();
        Map<String, Object> args = FsController.quien(req, "path", Db.str(body, "path"));
        args.put("now", ahora);
        args.put("lease_until", ahora + leaseSegundos * 1000);
        return raft.write("lock", args);
    }

    @PostMapping("/unlock")
    public Object unlock(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        return raft.write("unlock", FsController.quien(req, "path", Db.str(body, "path")));
    }

    // ===================== helpers =====================

    private List<Map<String, Object>> ubicaciones(List<DataNodes.Nodo> nodos) {
        List<Map<String, Object>> lista = new ArrayList<>();
        for (DataNodes.Nodo n : nodos) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("node_id", n.id);
            m.put("host", n.host);
            m.put("port", n.port);
            m.put("zona", n.zona);
            lista.add(m);
        }
        return lista;
    }
}
