package io.dfsha.controlnode.api;

import io.dfsha.controlnode.api.dto.Dtos;
import io.dfsha.controlnode.security.Principal;
import io.dfsha.controlnode.service.NamespaceService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** RF1: gestión del sistema de archivos (ls, mkdir, rm, mv, stat). */
@RestController
@RequestMapping("/api/v1/fs")
public class FsController {

    private final NamespaceService namespace;

    public FsController(NamespaceService namespace) { this.namespace = namespace; }

    @GetMapping("/list")
    public Map<String, Object> list(@RequestParam(defaultValue = "/") String path,
                                    @RequestAttribute("principal") Principal p) {
        List<NamespaceService.Entry> entries = namespace.list(path, p);
        return Map.of("path", path, "entries", entries);
    }

    @GetMapping("/stat")
    public NamespaceService.Entry stat(@RequestParam String path,
                                       @RequestAttribute("principal") Principal p) {
        return namespace.stat(path, p);
    }

    @PostMapping("/mkdir")
    public NamespaceService.Entry mkdir(@RequestBody Dtos.MkdirRequest body,
                                        @RequestAttribute("principal") Principal p) {
        return namespace.mkdir(body.path(), Boolean.TRUE.equals(body.parents()), p);
    }

    @DeleteMapping("/delete")
    public Map<String, Object> delete(@RequestParam String path,
                                      @RequestParam(defaultValue = "false") boolean recursive,
                                      @RequestAttribute("principal") Principal p) {
        namespace.delete(path, recursive, p);
        return Map.of("deleted", path, "recursive", recursive);
    }

    @PostMapping("/move")
    public NamespaceService.Entry move(@RequestBody Dtos.MoveRequest body,
                                       @RequestAttribute("principal") Principal p) {
        return namespace.move(body.src(), body.dst(), p);
    }
}
