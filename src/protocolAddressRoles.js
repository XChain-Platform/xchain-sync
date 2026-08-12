// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md.

// CONSENSUS-CRITICAL: protocol special-address canonicalization for the block
// hash preimage, a frozen mirror of xchain-indexer/src/protocolAddressRoles.js
// (which derives the same map from src/configs/*.js). The indexer hashes a
// protocol special address (BURN / GAS / DONATE1 / DONATE2 / REWARD) as its
// chain-independent role token rather than the per-chain address encoding, so
// identical actions hash identically on every chain; the replica has no
// per-coin config, hence the vendored snapshot. FEE_DESTINATION is excluded:
// native-fee mode writes no ledger credit/debit and it is env-overridable.
// Any divergence from the indexer copy makes the replica's recomputed hash
// mismatch the stored one and halts the divergence breaker, so keep the two
// byte-identical.

const ROLE_BY_ADDRESS = {
    // BTC
    '1XChainBurnAddressXXXXXXXXXbRsd2N': 'BURN',
    '1XChain3M4uRwcHqt4XuhVBUQ8cL4qQsA': 'GAS',
    '1Donate1GERVKPW6GFQcnGeTa8dgL6Abyp': 'DONATE1',
    '1Donate2LkbBrsanwCVRPWZCXAqQcvcqGz': 'DONATE2',
    '1rewardsZAyeuLeFJKoAepYiNN5N6uSzn': 'REWARD',
    'mxchainburnaddressXXXXXXXXXXa8EAfp': 'BURN',
    'mgassdEpzH2AuKGK9W5FZh8drWYKrpXk6D': 'GAS',
    'mfztXKX1HeVdCQf6pDCZFEzo5i5wYNHAM6': 'DONATE1',
    'myBbbZ4t7BPoyNcT4sHtFwZDuiyYGDXLQM': 'DONATE2',
    'mrewards4RQFYoZ5yEv4xr12PzfjDYViks': 'REWARD',
    'mgash6jYSKAR3Q5HPpDgNX2BYr18q9N6GQ': 'GAS',
    'muYHF9MMnK6Nmd5zx7EBtqEYZdaf2Xy8JX': 'DONATE1',
    'mkQd27aJSqsQ666z1Q4MLFmd3Ybqzy3TNw': 'DONATE2',
    'mrewardshQqD1ptkEBZGjPDF77L5uKJQmk': 'REWARD',
    // LTC
    'LXChainBurnAddressXXXXXXXXXXSkrYkJ': 'BURN',
    'LXChainCN6yjHVqqS9tYzYVYZ8CCZcSx72': 'GAS',
    'Ldonate18tNZcVThKm5MX33EjvhaanJ6Mg': 'DONATE1',
    'Ldonate2io846q2e7q8dUArh3TNnaq9ENb': 'DONATE2',
    'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX': 'REWARD',
    'mgashLN9oSvj2CUJYKWdNxh6VkamPg1Ges': 'GAS',
    'mybp5CceJvVV5tNCCiF7oBiZWko2fNkmnT': 'DONATE1',
    'muKEjejjXQvLY7Lp7Ecpn29gM2TCb5BLTF': 'DONATE2',
    'mgas5QYE38Bg34hwEjFKaE7Gs536FARue4': 'GAS',
    'mgNY2ZXbnNEkRT5ZRF8yGamivrSX2QH97h': 'DONATE1',
    'n2DLJPppXUi8jC6fLiSkthZi2sc9UKiZHd': 'DONATE2',
    // DOGE
    'DChainBurnAddressXXXXXXXXXXXawc9pt': 'BURN',
    'DGasfpttCnTijuuoAdiJ9sXJjG7vQ5pMkW': 'GAS',
    'DDonate1RBcwGnCRNnVtwuCmQyWW1Gn25f': 'DONATE1',
    'DDonate2o3Sg4phybp92oFpkmv8S9ZhGSV': 'DONATE2',
    'nchainburnaddressXXXXXXXXXXXYKgF7W': 'BURN',
    'ngasn6zHFzJ72zpk3DBKmXhD2XtszujSDW': 'GAS',
    'ndonate1dE87UXUFf4gjyhPg7hfQRJXVXr': 'DONATE1',
    'ndonate2wev8vKDgvd1DHhtJtvkRbn2usJ': 'DONATE2',
    'mvs8WdppEhzQLxfcYwrr1eoKA2nUFi55ff': 'BURN',
    'mgasDTdKu5DsbW97qSRnE8raAuYpKMfmhg': 'GAS',
    'mzdg8wGxgP3Jk45FuZPspumCL3Ruup37ob': 'DONATE1',
    'mmXU8RU7q3BUsyT66rtw1H6P7B2ZZd9c5Y': 'DONATE2',
};

// Any non-protocol address, including null, passes through unchanged.
function canonicalizeHashAddress(address) {
    if (address == null) return address;
    return ROLE_BY_ADDRESS[address] || address;
}

module.exports = { ROLE_BY_ADDRESS, canonicalizeHashAddress };
