package io.dfsha.controlnode.model;

/** Usuario del servicio. La contraseña se guarda como hash bcrypt. */
public class User {
    public String username;
    public String passwordHash;
    public String role;          // "admin" | "user"
    public String primaryGroup;
    public long createdAt;

    public User() {}

    public User(String username, String passwordHash, String role, String primaryGroup) {
        this.username = username;
        this.passwordHash = passwordHash;
        this.role = role;
        this.primaryGroup = primaryGroup;
        this.createdAt = System.currentTimeMillis();
    }

    public boolean isAdmin() { return "admin".equals(role); }
}
