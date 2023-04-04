# Scripts

This project includes some utility scripts. Script `manage-views` is 
installed with the package because it is used frequently. The other scripts
are not installed and to use them you must clone the repo. (This may change
if other scripts are deemed highly useful.)

## Long-running queries and keepalives

Certain scripts (e.g., `manage-views`) initiate very long-running queries.
Such queries can and do time out. To prevent this, use a connection string
including keepalive parameters.

The PostgreSQL documentation for 
[Database Connection Control Functions](https://www.postgresql.org/docs/current/libpq-connect.html) 
contains information on this.

The documentation lists connection functions and the parameters that can be 
passed to them. These parameters are listed in 
[section 34.1.2](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS), and include the following keepalive parameters.

> `keepalives`
*Controls whether client-side TCP keepalives* are used. The default value is 1, meaning on, but you can change this to 0, meaning off, if keepalives are not wanted. This parameter is ignored for connections made via a Unix-domain socket.
>
> `keepalives_idle`
Controls the number of seconds of inactivity after which TCP should send a keepalive message *to the server*. A value of zero uses the system default. This parameter is ignored for connections made via a Unix-domain socket, or if keepalives are disabled. It is only supported on systems where TCP_KEEPIDLE or an equivalent socket option is available, and on Windows; on other systems, it has no effect.
>
>`keepalives_interval`
Controls the number of seconds after which a TCP keepalive message that is not acknowledged by the server should be retransmitted. A value of zero uses the system default. This parameter is ignored for connections made via a Unix-domain socket, or if keepalives are disabled. It is only supported on systems where TCP_KEEPINTVL or an equivalent socket option is available, and on Windows; on other systems, it has no effect.
>
> `keepalives_count`
Controls the number of TCP keepalives that can be lost before the client's connection to the server is considered dead. A value of zero uses the system default. This parameter is ignored for connections made via a Unix-domain socket, or if keepalives are disabled. It is only supported on systems where TCP_KEEPCNT or an equivalent socket option is available; on other systems, it has no effect.

By setting these parameters to values that send keepalives at intervals 
shorter than the timeout interval (default 7200 s = 120 min, unchanged on
most of our systems), we can prevent the connection from being dropped.

Further, these parameters can be 
[set in the database connection strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING), 
which enables all clients to be set up appropriately without code change. 
For example:

```
postgresql://user:password@db.pcic.uvic.ca/crmp?keepalives=1&keepalives_idle=300&keepalives_interval=300&keepalives_count=9
```

The 300 s (5 min) interval is very conservative, but given the minimal overhead for keepalives, it should not cause problems.


## Script `manage-views`

Script `manage-views` enables the user to easily perform a refresh on 
"manual" materialized views, i.e., non-native matviews managed by PyCDS.
(Such matviews will soon be replaced by native ones, now that we are on
a reasonably recent version of PostgreSQL. But for now we need this script.)

Note that these refreshes are very long-running and will require keepalive
parameters in the connection string (see above) to prevent them being 
terminated by the system. For example

```text
manage-views -d 'postgresql://crmp@db.pcic.uvic.ca:5432/crmp?keepalives=1&keepalives_idle=300&keepalives_interval=300&keepalives_count=9' refresh all
```

Use the script help to get details on usage.

```
$ manage-views -h
usage: manage-views [-h] [-d DSN] [-s {NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL}] [-e {NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL}] {refresh} {daily,monthly-only,all}

Script to manage views in the PCDS/CRMP database. DSN strings are of form: dialect+driver://username:password@host:port/database Examples: postgresql://scott:tiger@localhost/mydatabase postgresql+psycopg2://scott:tiger@localhost/mydatabase
postgresql+pg8000://scott:tiger@localhost/mydatabase

positional arguments:
  {refresh}             Operation to perform
  {daily,monthly-only,all}
                        Views to affect

optional arguments:
  -h, --help            show this help message and exit
  -d DSN, --dsn DSN     Database DSN in which to manage views
  -s {NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL}, --scriptloglevel {NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Script logging level
  -e {NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL}, --dbengloglevel {NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Database engine logging level
```
