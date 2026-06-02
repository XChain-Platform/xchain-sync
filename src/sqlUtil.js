// Shared SQL-file helpers.
//
// Schema `.sql` files are split into individual statements on ';'. A ';' that
// appears inside a `--` line comment (prose punctuation in a column/header
// comment) must NOT be treated as a statement terminator — doing so truncates
// the CREATE TABLE into a bogus standalone query and silently fails schema
// creation. This is the exact bug xchain-indexer hit ("attests.sql's header
// split its comment, crash-looping the indexer"); keep this in sync with
// xchain-indexer/src/db.js#stripSqlLineComments.

// Remove `--` line comments while respecting quoted strings/identifiers, so a
// ';' inside a comment never reaches the statement splitter.
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

// Strip line comments, then split into trimmed, non-empty statements.
function splitSqlStatements(sql){
    return stripSqlLineComments(sql)
        .split(';')
        .map(q => q.trim())
        .filter(q => q !== '');
}

module.exports = { stripSqlLineComments, splitSqlStatements };
