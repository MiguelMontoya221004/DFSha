package io.dfsha.controlnode.service;

import io.dfsha.controlnode.api.DfshaException;
import io.dfsha.controlnode.config.ControlNodeProperties;
import io.dfsha.controlnode.metadata.MetadataStore;
import io.dfsha.controlnode.model.User;
import io.dfsha.controlnode.security.JwtService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

/** Registro y autenticación de usuarios (bcrypt + JWT). */
@Service
public class UserService {

    private static final Logger log = LoggerFactory.getLogger(UserService.class);

    private final MetadataStore store;
    private final NamespaceService namespace;
    private final JwtService jwt;
    private final ControlNodeProperties props;
    private final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder(12);

    public UserService(MetadataStore store, NamespaceService namespace,
                       JwtService jwt, ControlNodeProperties props) {
        this.store = store;
        this.namespace = namespace;
        this.jwt = jwt;
        this.props = props;
    }

    @PostConstruct
    void bootstrap() {
        store.write(() -> {
            if (store.getUser(props.adminUser()) == null) {
                store.putUser(new User(props.adminUser(), encoder.encode(props.adminPassword()),
                        "admin", "admins"));
                namespace.bootstrapDirectories(props.adminUser());
                log.info("Usuario administrador creado: {}", props.adminUser());
            }
            return null;
        });
    }

    public void register(String username, String password) {
        if (username == null || !username.matches("[a-zA-Z0-9_.-]{3,32}")) {
            throw DfshaException.badRequest("Usuario inválido: use 3-32 caracteres alfanuméricos");
        }
        if (password == null || password.length() < 6) {
            throw DfshaException.badRequest("La contraseña debe tener al menos 6 caracteres");
        }
        store.write(() -> {
            if (store.getUser(username) != null) {
                throw DfshaException.conflict("USER_EXISTS", "El usuario ya existe");
            }
            store.putUser(new User(username, encoder.encode(password), "user", username));
            namespace.createHome(username);
            log.info("Usuario registrado: {}", username);
            return null;
        });
    }

    public record Session(String token, long expiresIn, String username, String role) {}

    public Session login(String username, String password) {
        User user = store.read(() -> store.getUser(username));
        if (user == null || !encoder.matches(password, user.passwordHash)) {
            throw DfshaException.unauthorized("Usuario o contraseña incorrectos");
        }
        return new Session(jwt.issue(user.username, user.role), jwt.ttlSeconds(), user.username, user.role);
    }
}
