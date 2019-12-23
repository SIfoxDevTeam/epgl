#!/bin/bash
CN=$(lsb_release -c)
CodeName=${CN:10}
cat > /etc/apt/sources.list.d/2ndquadrant.list <<EOF
deb [arch=amd64] http://packages.2ndquadrant.com/pglogical/apt/ $CodeName-2ndquadrant main
EOF

wget --quiet -O - http://packages.2ndquadrant.com/pglogical/apt/AA7A6805.asc | sudo apt-key add -
  sudo apt-get update

V=$(psql --version)
VMajor=${V:18:3}
echo VMajor
sudo apt-get install postgresql-$VMajor-pglogical  
