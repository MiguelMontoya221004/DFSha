package io.dfsha.controlnode.model;

/**
 * Metadatos de un bloque. Las ubicaciones (réplicas) NO viven aquí: son estado
 * blando que el ControlNode reconstruye con los heartbeats y los reportes de
 * bloques de los DataNodes, igual que en HDFS.
 */
public class BlockMeta {
    public String blockId;
    public int index;
    public long size;
    public String sha256;

    public BlockMeta() {}

    public BlockMeta(String blockId, int index) {
        this.blockId = blockId;
        this.index = index;
    }
}
