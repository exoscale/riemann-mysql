# riemann-mysql

Watch mysql replication status and notify riemann of the output.

## Installation

### Dependencies

build dependencies (ubuntu names):

* Go compiler (>= 1.11)

### Building

For now, riemann-mysql is a source only distribution, you may build
it on linux by running make and make install.

### Package creation

run `make package` to build a debian package with `fpm`, you can
override the snapshot type version by supplying a VERSION environment
variable

## Configuration

The configuration file expects a simple `key = value` format,
empty lines are ignored, lines starting with a hash are ignored.

The following configuration directives are valid:

* `hostname`: hostname to use, otherwise, `gethostbyname`'s result is used
* `interval`: interval at which to run the query
* `delay`: delay to add to the interval before marking an event as expired
* `tags`: tags to add to the generated event
* `mysql_host`: mysql host to contact
* `mysql_user`: mysql user to connect as
* `mysql_password`: mysql password to use
* `mysql_database`: mysql database to bind to
* `mysql_port`: tcp port the mysql instance lives on
* `riemann_host`: host the riemann instance lives on
* `riemann_port`: tcp port the riemann instance lives on

## Running

riemann-mysql bundles an upstart script, letting you interact with it using
the service command.

## Caveats

This is a bare-bones release which makes the following assumptions:

* you have a tcp-server running on your riemann instance
* your mysql and riemann servers are reachable through ipv4


