/*********************************************************************
 *
 * Copyright © 2025–2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC – https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 ********************************************************************/

'use strict';

function classDigests(preimage, getDataHash){
    return Object.keys(preimage).map(key => {
        let value = preimage[key];
        return {
            class:  key,
            rows:   Array.isArray(value) ? value.length : null,
            digest: getDataHash({ class: key, value: value }).slice(0, 16)
        };
    });
}

module.exports = { classDigests };
