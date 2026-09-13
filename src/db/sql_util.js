// Copyright © 2025–2026 Dankest, LLC
// Based on XChain Platform by Dankest, LLC – https://dankest.llc
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of XChain Platform. Licensed under the GNU Affero
// General Public License v3.0 or later; see LICENSE.md. A commercial
// license (without AGPL source-disclosure terms) is available -
// contact legal@dankest.llc.

// Schema `.sql` files are split into individual statements on ';', so a ';'
// inside a `--` line comment (prose punctuation in a column or header comment)
// must never reach the splitter: it truncates the CREATE TABLE into a bogus
// standalone query and silently fails schema creation. xchain-indexer hit this
// exactly once (a header comment split its own attests.sql, crash-looping the
// indexer); keep in sync with xchain-indexer/src/db.js#stripSqlLineComments.
function stripSqlLineComments(sql){
    let out = '';
    let quote = null;
    for(let i = 0; i < sql.length; i++){
        const ch = sql[i];
        if(quote){
            out += ch;
            if(ch === quote){
                if(sql[i + 1] === quote){ out += sql[++i]; }
                else { quote = null; }
            }
            continue;
        }
        if(ch === "'" || ch === '"' || ch === '`'){ quote = ch; out += ch; continue; }
        if(ch === '-' && sql[i + 1] === '-'){
            while(i < sql.length && sql[i] !== '\n'){ i++; }
            if(i < sql.length){ out += '\n'; }
            continue;
        }
        out += ch;
    }
    return out;
}

function splitSqlStatements(sql){
    return stripSqlLineComments(sql)
        .split(';')
        .map(q => q.trim())
        .filter(q => q !== '');
}

module.exports = { stripSqlLineComments, splitSqlStatements };
