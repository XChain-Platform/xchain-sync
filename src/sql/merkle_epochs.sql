CREATE TABLE merkle_epochs (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    epoch       BIGINT NOT NULL UNIQUE,
    start_block BIGINT NOT NULL,
    end_block   BIGINT NOT NULL,
    merkle_root VARCHAR(64) NOT NULL,
    leaf_count  INT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
