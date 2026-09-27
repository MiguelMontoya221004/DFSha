package io.dfsha.controlnode.api.dto;

import java.util.List;

/** Cuerpos de petición de la API REST (los mismos nombres de campo que usa el cliente Python). */
public final class Dtos {

    private Dtos() {}

    public record Credentials(String username, String password) {}

    public record MkdirRequest(String path, Boolean parents) {}

    public record MoveRequest(String src, String dst) {}

    public record CreateFileRequest(String path, Boolean overwrite) {}

    public record AllocateBlockRequest(Integer index) {}

    public record CommitBlock(Integer index, String blockId, Long size, String sha256) {}

    public record CommitRequest(List<CommitBlock> blocks) {}

    public record OpenRequest(String path) {}
}
