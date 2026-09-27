package io.dfsha.controlnode.api;

import io.dfsha.controlnode.api.dto.Dtos;
import io.dfsha.controlnode.service.UserService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** RF4: registro y autenticación. Rutas públicas. */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    private final UserService users;

    public AuthController(UserService users) { this.users = users; }

    @PostMapping("/register")
    public ResponseEntity<Map<String, String>> register(@RequestBody Dtos.Credentials body) {
        users.register(body.username(), body.password());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("username", body.username(), "home", "/home/" + body.username()));
    }

    @PostMapping("/login")
    public UserService.Session login(@RequestBody Dtos.Credentials body) {
        return users.login(body.username(), body.password());
    }
}
