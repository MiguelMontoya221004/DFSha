package io.dfsha.controlnode.api;

import org.springframework.http.HttpStatus;

/** Error de negocio con código estable, para que el cliente Python lo interprete igual siempre. */
public class DfshaException extends RuntimeException {
    public final HttpStatus status;
    public final String code;

    public DfshaException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public static DfshaException notFound(String path) {
        return new DfshaException(HttpStatus.NOT_FOUND, "NOT_FOUND", "No existe: " + path);
    }

    public static DfshaException badRequest(String msg) {
        return new DfshaException(HttpStatus.BAD_REQUEST, "BAD_REQUEST", msg);
    }

    public static DfshaException conflict(String code, String msg) {
        return new DfshaException(HttpStatus.CONFLICT, code, msg);
    }

    public static DfshaException forbidden(String msg) {
        return new DfshaException(HttpStatus.FORBIDDEN, "FORBIDDEN", msg);
    }

    public static DfshaException unauthorized(String msg) {
        return new DfshaException(HttpStatus.UNAUTHORIZED, "UNAUTHORIZED", msg);
    }

    public static DfshaException unavailable(String msg) {
        return new DfshaException(HttpStatus.SERVICE_UNAVAILABLE, "NO_DATANODES", msg);
    }
}
