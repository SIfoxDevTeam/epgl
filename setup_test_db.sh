## Based on script of https://github.com/epgsql/epgsql
## Copyright (c) 2008, Will Glozer
## Copyright (c) 2011, Anton Lebedevich
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
##
##     * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
##     * Neither the name of Will Glozer nor the names of his contributors may be used to endorse or promote products derived from this software without specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#!/bin/sh

if [ -z $(which initdb) ] ; then
    echo "Postgres not found, you may need to launch like so: PATH=\$PATH:/usr/lib/postgresql/9.5/bin/ $0"
    exit 1
fi

## Thanks to Matwey V. Kornilov ( https://github.com/matwey ) for
## this:

initdb --locale en_US.UTF-8 datadir
cat > datadir/postgresql.conf <<EOF
lc_messages = 'en_US.UTF-8'
wal_level = 'logical'
max_replication_slots = 50
max_wal_senders = 50
shared_preload_libraries = 'pglogical'
log_statement = all
log_replication_commands = on
EOF

cat > datadir/pg_hba.conf <<EOF
local   all             $USER                        trust
host    template1	    $USER        127.0.0.1/32    trust
host    epgl_test_db    $USER        127.0.0.1/32    trust
host    epgl_test_db    epgl_test    127.0.0.1/32    trust
host    replication     epgl_test    127.0.0.1/32    trust
EOF
