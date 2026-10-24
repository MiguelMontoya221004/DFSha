package dfsha;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Lo que el ControlNode sabe de los DataNodes EN VIVO.
 *
 * Esta informacion no pasa por Raft a proposito: los heartbeats llegan cada 3
 * segundos y meterlos en el log replicado seria un desperdicio. Si el lider se
 * cae, el nuevo lider vuelve a conocer a todos los DataNodes en el siguiente
 * heartbeat. Lo que si va por Raft es el registro del nodo y en que DataNodes
 * quedo cada bloque.
 *
 * Aqui tambien viven:
 *   - la colocacion de replicas (zonas distintas + power of two choices)
 *   - la deteccion de nodos caidos
 *   - la cola de comandos que se le entrega a cada DataNode (re-replicar/borrar)
 */
@Component
public class DataNodes {

    private static final Logger log = LoggerFactory.getLogger(DataNodes.class);

    /** Estado en memoria de un DataNode. */
    public static class Nodo {
        public String id;
        public String host;
        /** Puerto donde el DataNode atiende al CLIENTE (subir/bajar bloques). */
        public int port;
        /** Puerto gRPC donde atiende a los OTROS DataNodes (pipeline). */
        public int peerPort;
        public String zona;
        public long capacidad;
        public long usado;
        public int bloques;
        public long ultimoHeartbeat;
        public final ConcurrentLinkedQueue<Map<String, Object>> comandos = new ConcurrentLinkedQueue<>();

        public long libre() {
            return Math.max(0, capacidad - usado);
        }
    }

    private final Map<String, Nodo> nodos = new ConcurrentHashMap<>();
    /** Bloques que ya se pidieron re-replicar, para no pedirlos otra vez cada 10 s. */
    private final Map<String, Long> rereplicacionPedida = new ConcurrentHashMap<>();
    private final Random random = new Random();
    private final RaftService raft;

    @Value("${dfsha.heartbeat-timeout-seconds}")
    private long timeoutSegundos;
    @Value("${dfsha.replication}")
    private int replicacion;

    public DataNodes(RaftService raft) {
        this.raft = raft;
    }

    // ===================== registro y heartbeats =====================

    public Nodo registrar(String id, String host, int port, int peerPort, String zona, long capacidad, long usado) {
        Nodo n = nodos.computeIfAbsent(id, k -> new Nodo());
        n.id = id;
        n.host = host;
        n.port = port;
        n.peerPort = peerPort;
        n.zona = zona;
        n.capacidad = capacidad;
        n.usado = usado;
        n.ultimoHeartbeat = System.currentTimeMillis();
        log.info("DataNode {} registrado en {}:{} (zona {})", id, host, port, zona);
        return n;
    }

    public Nodo heartbeat(String id, long usado, int bloques) {
        Nodo n = nodos.get(id);
        if (n == null) throw ApiException.notFound("el DataNode no esta registrado, vuelva a registrarse");
        n.usado = usado;
        n.bloques = bloques;
        n.ultimoHeartbeat = System.currentTimeMillis();
        return n;
    }

    public boolean vivo(String id) {
        Nodo n = nodos.get(id);
        return n != null && System.currentTimeMillis() - n.ultimoHeartbeat < timeoutSegundos * 1000;
    }

    public List<Nodo> vivos() {
        List<Nodo> lista = new ArrayList<>();
        for (Nodo n : nodos.values()) if (vivo(n.id)) lista.add(n);
        return lista;
    }

    public Nodo get(String id) {
        return nodos.get(id);
    }

    /** Lista para el endpoint de monitoreo. */
    public List<Map<String, Object>> estado() {
        List<Map<String, Object>> lista = new ArrayList<>();
        for (Nodo n : nodos.values()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("node_id", n.id);
            m.put("host", n.host);
            m.put("port", n.port);
            m.put("peer_port", n.peerPort);
            m.put("zona", n.zona);
            m.put("vivo", vivo(n.id));
            m.put("capacidad", n.capacidad);
            m.put("usado", n.usado);
            m.put("bloques", n.bloques);
            m.put("ultimo_heartbeat_hace_ms", System.currentTimeMillis() - n.ultimoHeartbeat);
            lista.add(m);
        }
        return lista;
    }

    // ===================== colocacion de replicas =====================

    /**
     * Escoge donde guardar las replicas de un bloque: primero reparte entre
     * zonas distintas y, dentro de cada zona, usa "power of two choices"
     * (saca dos nodos al azar y se queda con el que tenga mas espacio libre).
     */
    public List<Nodo> elegirReplicas(int cuantas, Set<String> excluir) {
        List<Nodo> candidatos = new ArrayList<>();
        for (Nodo n : vivos()) if (!excluir.contains(n.id)) candidatos.add(n);
        if (candidatos.isEmpty()) {
            throw ApiException.unavailable("no hay ningun DataNode vivo donde guardar el bloque");
        }
        // El numero de DataNodes es dinamico: si hay menos de los que pide el
        // factor de replica, se guarda en los que haya y el bloque queda
        // sub-replicado. Cuando entren mas DataNodes, la revision periodica
        // completa las copias que faltan. Negarse a escribir seria peor.
        int posibles = Math.min(cuantas, candidatos.size());
        if (posibles < cuantas) {
            log.warn("Solo hay {} DataNode(s) disponibles y se pedian {} replicas;"
                    + " el bloque queda sub-replicado hasta que entren mas nodos",
                    posibles, cuantas);
        }
        List<Nodo> elegidos = new ArrayList<>();
        Set<String> zonasUsadas = new HashSet<>();
        while (elegidos.size() < posibles && !candidatos.isEmpty()) {
            List<Nodo> preferidos = new ArrayList<>();
            for (Nodo n : candidatos) if (!zonasUsadas.contains(n.zona)) preferidos.add(n);
            List<Nodo> pozo = preferidos.isEmpty() ? candidatos : preferidos;
            Nodo elegido = elMasLibreDeDos(pozo);
            elegidos.add(elegido);
            zonasUsadas.add(elegido.zona);
            candidatos.remove(elegido);
        }
        return elegidos;
    }

    private Nodo elMasLibreDeDos(List<Nodo> pozo) {
        if (pozo.size() == 1) return pozo.get(0);
        Nodo a = pozo.get(random.nextInt(pozo.size()));
        Nodo b = pozo.get(random.nextInt(pozo.size()));
        return a.libre() >= b.libre() ? a : b;
    }

    // ===================== comandos para los DataNodes =====================

    public void encolar(String nodeId, Map<String, Object> comando) {
        Nodo n = nodos.get(nodeId);
        if (n != null) n.comandos.add(comando);
    }

    /** Los comandos pendientes se entregan en la respuesta del heartbeat. */
    public List<Map<String, Object>> comandosPendientes(String nodeId) {
        List<Map<String, Object>> lista = new ArrayList<>();
        Nodo n = nodos.get(nodeId);
        if (n != null) {
            Map<String, Object> c;
            while ((c = n.comandos.poll()) != null) lista.add(c);
        }
        return lista;
    }

    // ===================== re-replicacion automatica =====================

    /**
     * Cada 10 s el LIDER revisa si algun bloque quedo con menos de 3 replicas
     * vivas (porque se cayo un DataNode) y le pide a un nodo que todavia lo
     * tiene que lo copie a otro. Cuando el DataNode termina avisa con
     * /datanodes/block-received y ahi si se escribe la nueva replica en Raft.
     */
    @Scheduled(fixedDelayString = "10000", initialDelayString = "15000")
    @SuppressWarnings("unchecked")
    public void revisarSubReplicados() {
        if (!raft.esLider()) return;
        try {
            List<Map<String, Object>> bloques = (List<Map<String, Object>>) raft.read("all_replicas", Map.of());
            int pedidos = 0, sinDestino = 0;
            for (Map<String, Object> b : bloques) {
                List<String> enNodos = (List<String>) b.get("nodes");
                List<String> vivas = new ArrayList<>();
                for (String id : enNodos) if (vivo(id)) vivas.add(id);
                if (vivas.isEmpty() || vivas.size() >= replicacion) continue;

                // si ya se pidio hace poco, se le da tiempo al DataNode de copiarlo
                String blockId = (String) b.get("block_id");
                Long pedido = rereplicacionPedida.get(blockId);
                long ahora = System.currentTimeMillis();
                if (pedido != null && ahora - pedido < 60_000) continue;

                try {
                    List<Nodo> destinos = elegirReplicas(replicacion - vivas.size(), new HashSet<>(enNodos));
                    List<Map<String, Object>> targets = new ArrayList<>();
                    for (Nodo d : destinos) {
                        Map<String, Object> t = new LinkedHashMap<>();
                        t.put("node_id", d.id);
                        t.put("host", d.host);
                        t.put("peer_port", d.peerPort);
                        targets.add(t);
                    }
                    Map<String, Object> comando = new LinkedHashMap<>();
                    comando.put("tipo", "REPLICATE_BLOCK");
                    comando.put("block_id", blockId);
                    comando.put("targets", targets);
                    encolar(vivas.get(0), comando);
                    // el periodo de gracia se marca solo si el comando SI se encolo
                    rereplicacionPedida.put(blockId, ahora);
                    pedidos++;
                } catch (ApiException e) {
                    // no hay a donde copiarlo (no quedan nodos libres vivos): se
                    // deja para la proxima revision y se sigue con los demas
                    sinDestino++;
                }
            }
            if (sinDestino > 0) {
                log.warn("Hay {} bloques sub-replicados sin DataNode libre donde copiarlos", sinDestino);
            }
            if (pedidos > 0) log.warn("Re-replicacion: se pidieron {} bloques sub-replicados", pedidos);
        } catch (ApiException e) {
            log.debug("No se pudo revisar la re-replicacion: {}", e.getMessage());
        }
    }

    /** El DataNode confirmo que ya copio el bloque: se puede volver a revisar. */
    public void replicaConfirmada(String blockId) {
        rereplicacionPedida.remove(blockId);
    }

    /** Bloques que ahora mismo tienen menos replicas vivas de las que deberian. */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> subReplicados() {
        List<Map<String, Object>> bloques = (List<Map<String, Object>>) raft.read("all_replicas", Map.of());
        List<Map<String, Object>> malos = new ArrayList<>();
        for (Map<String, Object> b : bloques) {
            List<String> vivas = new ArrayList<>();
            for (String id : (List<String>) b.get("nodes")) if (vivo(id)) vivas.add(id);
            if (vivas.size() < replicacion) {
                Map<String, Object> m = new LinkedHashMap<>(b);
                m.put("replicas_vivas", vivas);
                malos.add(m);
            }
        }
        return malos;
    }
}
