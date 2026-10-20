-- Copyright © 2025-2026 Dankest, LLC
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- migration-key: source-column
ALTER TABLE `{{table}}` {{actions}};

-- migration-key: orders-give-ownership
ALTER TABLE `orders` ADD COLUMN `give_ownership` TINYINT(1) NOT NULL DEFAULT 0;
-- migration-key: orders-get-ownership
ALTER TABLE `orders` ADD COLUMN `get_ownership` TINYINT(1) NOT NULL DEFAULT 0;
-- migration-key: swaps-give-ownership
ALTER TABLE `swaps` ADD COLUMN `give_ownership` TINYINT(1) NOT NULL DEFAULT 0;
-- migration-key: swaps-get-ownership
ALTER TABLE `swaps` ADD COLUMN `get_ownership` TINYINT(1) NOT NULL DEFAULT 0;
-- migration-key: state-tree-roots-contract-state-root
ALTER TABLE `state_tree_roots` ADD COLUMN `contract_state_root` CHAR(64) NULL AFTER `block_merkle_root`;
-- migration-key: state-tree-roots-contract-state-root-shadow
ALTER TABLE `state_tree_roots` ADD COLUMN `contract_state_root_shadow` CHAR(64) NULL AFTER `contract_state_root`;
-- migration-key: state-tree-roots-balances-root-escrow-shadow
ALTER TABLE `state_tree_roots` ADD COLUMN `balances_root_escrow_shadow` CHAR(64) NULL AFTER `contract_state_root_shadow`;
-- migration-key: anchor-actions-section-index
ALTER TABLE `anchor_actions` ADD COLUMN IF NOT EXISTS `section_index` TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER `action_index`;
-- migration-key: anchor-actions-section-index-add
ALTER TABLE `anchor_actions` ADD COLUMN `section_index` TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER `action_index`;
-- migration-key: validator-rewards-round-qualifier
ALTER TABLE `validator_rewards` ADD COLUMN IF NOT EXISTS `round_qualifier` BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `round_reference`;
-- migration-key: validator-rewards-round-qualifier-add
ALTER TABLE `validator_rewards` ADD COLUMN `round_qualifier` BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `round_reference`;
-- migration-key: anchor-reward-reconcile-round-qualifier
ALTER TABLE `anchor_reward_reconcile_log` ADD COLUMN IF NOT EXISTS `round_qualifier` BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `round_reference`;
-- migration-key: anchor-reward-reconcile-round-qualifier-add
ALTER TABLE `anchor_reward_reconcile_log` ADD COLUMN `round_qualifier` BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `round_reference`;
-- migration-key: contract-emissions-action-index-nullable
ALTER TABLE `contract_emissions` MODIFY COLUMN `action_index` BIGINT UNSIGNED NULL;
-- migration-key: repair-auto-increment
ALTER TABLE `{{table}}` MODIFY id BIGINT NOT NULL AUTO_INCREMENT;
-- migration-key: attests-request-status
ALTER TABLE `attests` MODIFY COLUMN `request_status` ENUM('pending','fulfilled','expired','errored','rejected') NOT NULL DEFAULT 'pending';
-- migration-key: utf8mb4-columns
ALTER TABLE `{{table}}` {{clauses}};
