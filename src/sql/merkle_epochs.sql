--********************************************************************
--
-- Copyright © 2025-2026 Dankest, LLC
-- Based on XChain Platform by Dankest, LLC - https://dankest.llc
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of XChain Platform. Licensed under the GNU Affero
-- General Public License v3.0 or later; see LICENSE.md. A commercial
-- license (without AGPL source-disclosure terms) is available -
-- contact legal@dankest.llc.
--
--********************************************************************

CREATE TABLE merkle_epochs (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    epoch       BIGINT NOT NULL UNIQUE,
    start_block BIGINT NOT NULL,
    end_block   BIGINT NOT NULL,
    merkle_root VARCHAR(64) NOT NULL,
    leaf_count  INT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
