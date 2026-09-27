package io.dfsha.controlnode.metadata;

import io.dfsha.controlnode.model.FileVersion;
import io.dfsha.controlnode.model.INode;
import io.dfsha.controlnode.model.User;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * Almacén de metadatos del ControlNode.
 *
 * Toda mutación del espacio de nombres pasa por {@link #write(Supplier)} y toda
 * consulta por {@link #read(Supplier)}. Ese es el punto de extensión del Hito 3:
 * la implementación con Apache Ratis convertirá cada write() en una entrada del
 * log de Raft replicada por mayoría, sin tocar la lógica de los servicios.
 *
 * Operaciones que serán entradas del log de Raft:
 *   CreateUser · Mkdir · Delete · Move · CreateVersion · AllocateBlock ·
 *   CommitVersion · AbortVersion · AcquireLease · ReleaseLease
 */
public interface MetadataStore {

    <T> T write(Supplier<T> operation);

    <T> T read(Supplier<T> operation);

    // ---- usuarios ----
    User getUser(String username);
    void putUser(User user);
    Collection<User> users();

    // ---- espacio de nombres ----
    INode root();

    /** @return el INode de la ruta o null si no existe. */
    INode resolve(String path);

    // ---- versiones de archivo ----
    void putVersion(FileVersion version);
    FileVersion getVersion(String versionId);
    void removeVersion(String versionId);
    Collection<FileVersion> versions();
}
