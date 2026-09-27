package io.dfsha.controlnode.api;

import io.dfsha.controlnode.api.dto.Dtos;
import io.dfsha.controlnode.security.Principal;
import io.dfsha.controlnode.service.FileService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * RF2 y RF3: ciclo de vida de los archivos.
 * El ControlNode nunca ve los bytes: sólo asigna bloques y devuelve pipelines y tokens.
 */
@RestController
@RequestMapping("/api/v1")
public class FilesController {

    private final FileService files;

    public FilesController(FileService files) { this.files = files; }

    @PostMapping("/files/create")
    public FileService.CreateResult create(@RequestBody Dtos.CreateFileRequest body,
                                           @RequestAttribute("principal") Principal p) {
        return files.create(body.path(), Boolean.TRUE.equals(body.overwrite()), p);
    }

    @PostMapping("/versions/{versionId}/blocks")
    public FileService.AllocatedBlock allocate(@PathVariable String versionId,
                                               @RequestBody Dtos.AllocateBlockRequest body,
                                               @RequestAttribute("principal") Principal p) {
        if (body.index() == null) throw DfshaException.badRequest("Falta el índice del bloque");
        return files.allocateBlock(versionId, body.index(), p);
    }

    @PostMapping("/versions/{versionId}/commit")
    public FileService.CommitResult commit(@PathVariable String versionId,
                                           @RequestBody Dtos.CommitRequest body,
                                           @RequestAttribute("principal") Principal p) {
        if (body.blocks() == null) throw DfshaException.badRequest("Falta la lista de bloques");
        List<FileService.CommittedBlock> blocks = body.blocks().stream()
                .map(b -> new FileService.CommittedBlock(b.index(), b.blockId(), b.size(), b.sha256()))
                .toList();
        return files.commit(versionId, blocks, p);
    }

    @PostMapping("/versions/{versionId}/abort")
    public Map<String, String> abort(@PathVariable String versionId,
                                     @RequestAttribute("principal") Principal p) {
        files.abort(versionId, p);
        return Map.of("aborted", versionId);
    }

    @PostMapping("/files/open")
    public FileService.OpenResult open(@RequestBody Dtos.OpenRequest body,
                                       @RequestAttribute("principal") Principal p) {
        return files.open(body.path(), p);
    }
}
