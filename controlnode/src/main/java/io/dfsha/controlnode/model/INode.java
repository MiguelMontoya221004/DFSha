package io.dfsha.controlnode.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Nodo del espacio de nombres: directorio o archivo. */
public class INode {
    public enum Type { DIR, FILE }

    public String name;
    public String path;
    public Type type;
    public String owner;
    public String group;
    /** Permisos tipo POSIX: 9 caracteres, p. ej. "rwxr-x---". */
    public String permissions;
    public long createdAt = System.currentTimeMillis();
    public long modifiedAt = System.currentTimeMillis();

    // Directorios
    public Map<String, INode> children = new LinkedHashMap<>();

    // Archivos
    public String fileId;
    public FileVersion current;                       // última versión confirmada
    public List<FileVersion> history = new ArrayList<>();
    public Lease lease;                               // lock de escritura vigente

    public INode() {}

    public INode(String name, String path, Type type, String owner, String group, String permissions) {
        this.name = name;
        this.path = path;
        this.type = type;
        this.owner = owner;
        this.group = group;
        this.permissions = permissions;
    }

    public boolean isDir() { return type == Type.DIR; }

    public long size() { return current != null ? current.size : 0L; }
}
