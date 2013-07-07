" Vim global plugin for formatting sql in the selected range
" Maintainer: https://github.com/chaosong
" License: DO WHAT THE FUCK YOU WANT TO DO

if exists("sql_format")
    finish
endif
let sql_format = 1

if !has('python')
    echo "Error: Required vim compiled with +python"
    finish
endif

vmap <f6> :FormatSQL<cr>
command -range FormatSQL :call FormatSQL(<line1>, <line2>)

function! FormatSQL(start, end)

python << EOF

import vim
import sqlparse

start = int(vim.eval('a:start')) - 1
end = int(vim.eval('a:end')) - 1
buf = vim.current.buffer
NL = '\n'

try:
    sql = NL.join(buf[start:end + 1])
    sql_new = sqlparse.format(sql, reindent=True, keyword_case='upper')

    lines = [line.encode('utf-8') for line in sql_new.split(NL)]
    buf[:] = buf[:start] + lines + buf[end + 1:]
except Exception, e:
    print e
EOF

endfunction

