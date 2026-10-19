-- Copyright © 2025-2026 Dankest, LLC
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- migration-key: index-tickers-block-index
ALTER TABLE `index_tickers` ADD INDEX `block_index` (block_index);
-- migration-key: index-addresses-block-index
ALTER TABLE `index_addresses` ADD INDEX `block_index` (block_index);
-- migration-key: state-tree-roots-block-index
ALTER TABLE `state_tree_roots` ADD INDEX `block_index` (block_index);
-- migration-key: credits-credits-address-tick-amount
ALTER TABLE `credits` ADD INDEX `credits_address_tick_amount` (address_id, tick_id, amount);
-- migration-key: debits-debits-address-tick-amount
ALTER TABLE `debits` ADD INDEX `debits_address_tick_amount` (address_id, tick_id, amount);
-- migration-key: attests-version-resolved
ALTER TABLE `attests` ADD INDEX `version_resolved` (version, resolved_block);
-- migration-key: xcalls-version-resolved
ALTER TABLE `xcalls` ADD INDEX `version_resolved` (version, resolved_block);
-- migration-key: polls-resolved-block
ALTER TABLE `polls` ADD INDEX `resolved_block` (resolved_block);
-- migration-key: polls-callback-due-block
ALTER TABLE `polls` ADD INDEX `callback_due_block` (callback_due_block);
-- migration-key: anchor-actions-idx-anchor-block-doge
ALTER TABLE `anchor_actions` ADD INDEX `idx_anchor_block_doge` (block_index_doge);
-- migration-key: state-checkpoints-idx-chain-block
ALTER TABLE `state_checkpoints` ADD INDEX `idx_chain_block` (chain, network, block_index);
-- migration-key: attests-drop-request-id-version
ALTER TABLE `attests` DROP INDEX `request_id_version`;
-- migration-key: attests-request-id-version
CREATE INDEX `request_id_version` ON `attests` (request_id, version);
-- migration-key: votes-drop-poll-voter-choice
ALTER TABLE `votes` DROP INDEX `poll_voter_choice`;
-- migration-key: votes-poll-voter-action-choice
CREATE UNIQUE INDEX `poll_voter_action_choice` ON `votes` (poll_index, voter_address_id, action_index, choice);
-- migration-key: anchor-actions-primary-key
ALTER TABLE `anchor_actions` DROP PRIMARY KEY, ADD PRIMARY KEY (`action_index`, `section_index`);
-- migration-key: validator-rewards-reward-unique
ALTER TABLE `validator_rewards` DROP INDEX `reward_unique`, ADD UNIQUE INDEX `reward_unique` (`source_id`, `signing_pubkey_id`, `reward_type`, `round_reference`, `round_qualifier`);
