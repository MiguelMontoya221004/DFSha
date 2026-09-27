package io.dfsha.controlnode.security;

/** Identidad autenticada que viaja en el JWT. */
public record Principal(String username, String role) {
    public boolean isAdmin() { return "admin".equals(role); }
}
