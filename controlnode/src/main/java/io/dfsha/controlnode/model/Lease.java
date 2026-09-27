package io.dfsha.controlnode.model;

/** Lock de escritura con expiración: garantiza un solo escritor por archivo. */
public class Lease {
    public String leaseId;
    public String owner;      // usuario
    public String versionId;  // versión borrador asociada
    public long expiresAt;

    public Lease() {}

    public Lease(String leaseId, String owner, String versionId, long ttlMillis) {
        this.leaseId = leaseId;
        this.owner = owner;
        this.versionId = versionId;
        this.expiresAt = System.currentTimeMillis() + ttlMillis;
    }

    public boolean expired() { return System.currentTimeMillis() > expiresAt; }
}
