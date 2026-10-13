package dfsha;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * ControlNode de DFSha.
 *
 * Cada instancia levanta tres cosas:
 *   1. La API REST para el cliente (Spring Boot, puerto 8443).
 *   2. Un servidor Raft (Apache Ratis) que replica los metadatos con los otros
 *      dos ControlNodes.
 *   3. El registro de DataNodes vivos + la re-replicacion automatica.
 *
 * Por el ControlNode NUNCA pasan los bytes de los archivos: solo dice en que
 * DataNodes esta cada bloque y entrega un token firmado para poder leerlo o
 * escribirlo.
 */
@SpringBootApplication
@EnableScheduling
public class ControlNodeApp {

    public static void main(String[] args) {
        SpringApplication.run(ControlNodeApp.class, args);
    }
}

/** Convierte las ApiException en respuestas JSON con su codigo HTTP. */
@RestControllerAdvice
class ErrorHandler {

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, Object>> handle(ApiException e) {
        return ResponseEntity.status(e.status).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handle(Exception e) {
        String msg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return ResponseEntity.status(500).body(Map.of("error", msg));
    }
}
