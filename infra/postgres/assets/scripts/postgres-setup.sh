#!/bin/bash
export DEBIAN_FRONTEND=noninteractive
apt-get -y update
apt-get upgrade
apt-get -y install sudo
apt-get install -y gnupg2
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt update
apt-get -y install postgresql-17
#apt-get -y install postgresql=14+238 postgresql-contrib=14+238
# Start Postgres service
sudo service postgresql start
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD '123';"
sudo -u postgres psql -c "CREATE DATABASE proply;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE proply TO postgres;"
sudo -u postgres psql -d proply -c "CREATE SCHEMA staging;"
sudo -u postgres psql -d proply -c "CREATE SCHEMA dbt_warehouse;"
# Modify Postgres config with assets
cp -f /assets/config/pg_hba.conf /etc/postgresql/17/main/pg_hba.conf 
cp -f /assets/config/postgresql.conf /etc/postgresql/17/main/postgresql.conf 


# Install PostGIS, pgRouting
sudo apt install postgresql-17-postgis-3
sudo apt install postgis
sudo -u postgres psql -c "ALTER DATABASE proply SET search_path=public,postgis,contrib;"
sudo -u postgres psql -d proply -c "CREATE SCHEMA postgis;"
sudo -u postgres psql -d proply -c "CREATE EXTENSION postgis SCHEMA postgis;"

sudo service postgresql restart
