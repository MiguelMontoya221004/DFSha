package dfsha;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Puente entre Raft (Apache Ratis) y los metadatos.
 *
 * Ratis llama a applyTransaction() cuando un comando ya quedo escrito en el log
 * de la mayoria de los nodos; como los 3 ControlNodes aplican los mismos
 * comandos en el mismo orden, las 3 bases de datos quedan iguales.
 *
 * query() se usa para las lecturas: Ratis solo la ejecuta en el lider, por eso
 * las lecturas son linealizables.
 */
@Component
public class MetadataStateMachine extends BaseStateMachine {

    private static final Logger log = LoggerFactory.getLogger(MetadataStateMachine.class);

    private final Db db;
    private final ObjectMapper json = new ObjectMapper();

    public MetadataStateMachine(Db db) {
        this.db = db;
    }

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
        super.initialize(server, groupId, storage);
        // La base de datos guarda hasta que comando alcanzo a aplicar, asi al
        // reiniciar no se repiten los comandos que ya estaban guardados.
        long[] aplicado = db.applied();
        if (aplicado[1] >= 0) {
            setLastAppliedTermIndex(TermIndex.valueOf(aplicado[0], aplicado[1]));
            log.info("Metadatos locales al dia hasta el indice {} de Raft", aplicado[1]);
        }
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        return CompletableFuture.completedFuture(
                ejecutar(request.getContent().toStringUtf8(), -1, -1, false));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        LogEntryProto entrada = trx.getLogEntry();
        String payload = entrada.getStateMachineLogEntry().getLogData().toStringUtf8();
        Message respuesta = ejecutar(payload, entrada.getTerm(), entrada.getIndex(), true);
        updateLastAppliedTermIndex(entrada.getTerm(), entrada.getIndex());
        return CompletableFuture.completedFuture(respuesta);
    }

    /** Ejecuta un comando o una consulta y devuelve la respuesta en JSON. */
    @SuppressWarnings("unchecked")
    private Message ejecutar(String payload, long term, long index, boolean escritura) {
        Map<String, Object> salida = new LinkedHashMap<>();
        try {
            Map<String, Object> peticion = json.readValue(payload, new TypeReference<Map<String, Object>>() {});
            String op = (String) peticion.get("op");
            Map<String, Object> args = (Map<String, Object>) peticion.getOrDefault("args", Map.of());
            Object data = escritura ? db.apply(term, index, op, args) : db.query(op, args);
            salida.put("ok", true);
            salida.put("data", data);
        } catch (ApiException e) {
            salida.put("ok", false);
            salida.put("status", e.status);
            salida.put("error", e.getMessage());
        } catch (Exception e) {
            log.error("Error aplicando el comando", e);
            salida.put("ok", false);
            salida.put("status", 500);
            salida.put("error", String.valueOf(e.getMessage()));
        }
        try {
            return Message.valueOf(json.writeValueAsString(salida));
        } catch (Exception e) {
            return Message.valueOf("{\"ok\":false,\"status\":500,\"error\":\"no se pudo serializar la respuesta\"}");
        }
    }
}
