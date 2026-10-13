-- Esquema de metadatos del ControlNode.
-- Cada nodo tiene su propia copia; Raft garantiza que las 3 copias
-- apliquen exactamente los mismos comandos en el mismo orden.

-- Ultimo comando de Raft aplicado a ESTA base de datos.
CREATE TABLE IF NOT EXISTS applied (
    id    INT PRIMARY KEY,
    term  BIGINT NOT NULL,
    idx   BIGINT NOT NULL
);
MERGE INTO applied (id, term, idx) KEY(id) VALUES (1, -1, -1);

CREATE TABLE IF NOT EXISTS users (
    username      VARCHAR(64) PRIMARY KEY,
    password_hash VARCHAR(100) NOT NULL,
    group_name    VARCHAR(64) NOT NULL,
    created_at    BIGINT NOT NULL
);

-- Namespace: directorios y archivos
CREATE TABLE IF NOT EXISTS files (
    path        VARCHAR(1024) PRIMARY KEY,
    parent      VARCHAR(1024) NOT NULL,
    name        VARCHAR(255)  NOT NULL,
    is_dir      BOOLEAN NOT NULL,
    owner       VARCHAR(64) NOT NULL,
    group_name  VARCHAR(64) NOT NULL,
    perms       INT NOT NULL,              -- permisos POSIX en octal, ej. 755
    size        BIGINT NOT NULL DEFAULT 0,
    version     INT NOT NULL DEFAULT 0,    -- version publicada (0 = archivo vacio)
    created_at  BIGINT NOT NULL,
    modified_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent);

-- Bloques de cada version del archivo (copy-on-write: las versiones viejas
-- se conservan hasta que el recolector las borre)
CREATE TABLE IF NOT EXISTS blocks (
    block_id VARCHAR(64) PRIMARY KEY,
    path     VARCHAR(1024) NOT NULL,
    version  INT NOT NULL,
    idx      INT NOT NULL,
    size     BIGINT NOT NULL,
    checksum VARCHAR(80) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_blocks_version ON blocks(path, version, idx);

-- En que DataNodes esta cada bloque
CREATE TABLE IF NOT EXISTS replicas (
    block_id VARCHAR(64) NOT NULL,
    node_id  VARCHAR(64) NOT NULL,
    PRIMARY KEY (block_id, node_id)
);

-- DataNodes registrados en el cluster
CREATE TABLE IF NOT EXISTS datanodes (
    node_id       VARCHAR(64) PRIMARY KEY,
    host          VARCHAR(128) NOT NULL,
    port          INT NOT NULL,
    zone          VARCHAR(32) NOT NULL,
    capacity      BIGINT NOT NULL,
    registered_at BIGINT NOT NULL
);

-- Escrituras en curso (una version se publica solo al hacer commit)
CREATE TABLE IF NOT EXISTS uploads (
    upload_id  VARCHAR(64) PRIMARY KEY,
    path       VARCHAR(1024) NOT NULL,
    version    INT NOT NULL,
    owner      VARCHAR(64) NOT NULL,
    started_at BIGINT NOT NULL
);

-- Un solo escritor por archivo, con lease que vence
CREATE TABLE IF NOT EXISTS locks (
    path       VARCHAR(1024) PRIMARY KEY,
    owner      VARCHAR(64) NOT NULL,
    expires_at BIGINT NOT NULL
);

-- Raiz del espacio de nombres. Se crea igual en los 3 nodos, por eso va aqui
-- y no como un comando de Raft.
MERGE INTO files (path,parent,name,is_dir,owner,group_name,perms,size,version,created_at,modified_at)
  KEY(path) VALUES ('/','/','/',TRUE,'root','root',755,0,0,0,0);
MERGE INTO files (path,parent,name,is_dir,owner,group_name,perms,size,version,created_at,modified_at)
  KEY(path) VALUES ('/home','/','home',TRUE,'root','root',755,0,0,0,0);
MERGE INTO files (path,parent,name,is_dir,owner,group_name,perms,size,version,created_at,modified_at)
  KEY(path) VALUES ('/shared','/','shared',TRUE,'root','root',777,0,0,0,0);
