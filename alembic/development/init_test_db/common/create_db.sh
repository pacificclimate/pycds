database=$1
owner=$2

cat <<EOT
-- Database $database

CREATE DATABASE $database WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_CA.UTF-8' LC_CTYPE = 'en_CA.UTF-8';
ALTER DATABASE $database OWNER TO $owner;

EOT
