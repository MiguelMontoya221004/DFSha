package io.dfsha.controlnode.model;

/** Estado de un DataNode registrado, actualizado con cada heartbeat. */
public class DataNodeInfo {
    public String nodeId;
    public String address;        // "host:puerto" anunciado por el propio DataNode
    public String zone;           // máquina física / zona de disponibilidad
    public long capacityBytes;
    public long usedBytes;
    public long freeBytes;
    public int activeTransfers;
    public int blockCount;
    public long registeredAt = System.currentTimeMillis();
    public long lastHeartbeat = System.currentTimeMillis();
    public boolean alive = true;

    public DataNodeInfo() {}

    public DataNodeInfo(String nodeId, String address, String zone, long capacityBytes) {
        this.nodeId = nodeId;
        this.address = address;
        this.zone = zone;
        this.capacityBytes = capacityBytes;
        this.freeBytes = capacityBytes;
    }

    /**
     * Puntaje de carga usado por el algoritmo de colocación: 0 = nodo libre.
     * 60 % ocupación de disco + 40 % transferencias activas.
     */
    public double loadScore() {
        double diskRatio = capacityBytes > 0 ? (double) usedBytes / capacityBytes : 1.0;
        double transferRatio = Math.min(1.0, activeTransfers / 8.0);
        return 0.6 * diskRatio + 0.4 * transferRatio;
    }
}
