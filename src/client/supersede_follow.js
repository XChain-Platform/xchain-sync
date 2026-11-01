// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

'use strict';

// Pure rules for identifying binding steps in a superseding checkpoint range.
function supersedingSeqFor(step, chain){
    const stepSeq = Number(step.checkpoint_seq);
    const stepBlock = Number(step.block_index);
    let lowest = null;

    for(const candidate of chain){
        if(candidate.supersedes_from === null || candidate.supersedes_from === undefined) continue;

        const candidateSeq = Number(candidate.checkpoint_seq);
        if(candidateSeq <= stepSeq || Number(candidate.supersedes_from) > stepBlock) continue;
        if(lowest === null || candidateSeq < lowest) lowest = candidateSeq;
    }

    return lowest;
}

function isNonBindingStep(step, chain){
    return supersedingSeqFor(step, chain) !== null;
}

function bindingSteps(chain){
    if(!Array.isArray(chain)) throw new TypeError('chain must be an array');
    return chain.filter((step) => !isNonBindingStep(step, chain));
}

module.exports = { supersedingSeqFor, isNonBindingStep, bindingSteps };
