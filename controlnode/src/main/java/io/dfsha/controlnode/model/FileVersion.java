package io.dfsha.controlnode.model;

import java.util.Map;
import java.util.TreeMap;

/**
 * Versión inmutable de un archivo. Una escritura crea una versión DRAFT y sólo
 * al confirmarla (commit) pasa a COMMITTED y se vuelve visible: así la
 * publicación de un archivo es atómica (copy-on-write).
 */
public class FileVersion {
    public enum State { DRAFT, COMMITTED, ABORTED }

    public String versionId;
    public String fileId;
    public String path;
    public State state = State.DRAFT;
    public int blockSize;
    public long size;
    public String createdBy;
    public long createdAt = System.currentTimeMillis();
    public long committedAt;
    public Map<Integer, BlockMeta> blocks = new TreeMap<>();

    public FileVersion() {}

    public FileVersion(String versionId, String fileId, String path, int blockSize, String createdBy) {
        this.versionId = versionId;
        this.fileId = fileId;
        this.path = path;
        this.blockSize = blockSize;
        this.createdBy = createdBy;
    }
}
