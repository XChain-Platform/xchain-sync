/*********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 **********************************************************************
 *
 * XChain Sync - checkpoint supersession suffix, at the SDK's path
 *
 * src/checkpoint.js is a code-identical twin of the SDK copy, so its require
 * strings must match the SDK layout. The SDK keeps the suffix module here; the
 * sync implementation lives in src/client/supersede_suffix.js and is re-exported
 * so both requires resolve to one module.
 *
 ********************************************************************/

'use strict';

module.exports = require('../../client/supersede_suffix.js');
