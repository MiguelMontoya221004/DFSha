package io.dfsha.controlnode.metadata;

import io.dfsha.controlnode.model.FileVersion;
import io.dfsha.controlnode.model.INode;
import io.dfsha.controlnode.model.User;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * Implementación en memoria (Hito 2). Un ReentrantReadWriteLock da el orden
 * total de las operaciones; en el Hito 2 el ControlNode es único, así que esto
 * es linealizable dentro del proceso. En el Hito 3 se reemplaza por Raft.
 */
@Component
public class InMemoryMetadataStore implements MetadataStore {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, User> users = new ConcurrentHashMap<>();
    private final Map<String, FileVersion> versions = new ConcurrentHashMap<>();
    private final INode root = new INode("/", "/", INode.Type.DIR, "admin", "admin", "rwxr-xr-x");

    @Override
    public <T> T write(Supplier<T> operation) {
        lock.writeLock().lock();
        try {
            return operation.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public <T> T read(Supplier<T> operation) {
        lock.readLock().lock();
        try {
            return operation.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public User getUser(String username) { return users.get(username); }
    @Override public void putUser(User user) { users.put(user.username, user); }
    @Override public Collection<User> users() { return users.values(); }

    @Override public INode root() { return root; }

    @Override
    public INode resolve(String path) {
        String normalized = normalize(path);
        if (normalized.equals("/")) return root;
        INode current = root;
        for (String part : normalized.substring(1).split("/")) {
            if (part.isEmpty()) continue;
            if (!current.isDir()) return null;
            current = current.children.get(part);
            if (current == null) return null;
        }
        return current;
    }

    @Override public void putVersion(FileVersion v) { versions.put(v.versionId, v); }
    @Override public FileVersion getVersion(String versionId) { return versions.get(versionId); }
    @Override public void removeVersion(String versionId) { versions.remove(versionId); }
    @Override public Collection<FileVersion> versions() { return versions.values(); }

    /** Normaliza rutas: colapsa "//", resuelve "." y "..", quita la barra final. */
    public static String normalize(String path) {
        if (path == null || path.isBlank()) return "/";
        String p = path.replace('\\', '/');
        if (!p.startsWith("/")) p = "/" + p;
        java.util.Deque<String> stack = new java.util.ArrayDeque<>();
        for (String part : p.split("/")) {
            if (part.isEmpty() || part.equals(".")) continue;
            if (part.equals("..")) { stack.pollLast(); continue; }
            stack.addLast(part);
        }
        return stack.isEmpty() ? "/" : "/" + String.join("/", stack);
    }
}
