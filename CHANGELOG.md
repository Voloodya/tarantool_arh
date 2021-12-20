# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.3.0] - 2021-10-27

- update Cartridge to 2.7.3
- allow to specify custom timeout for map_reduce and call_on_storage functions
- rename "expiration" configuration section to "versioning". 
Usage of "expiration" section is deprecated.
- add expiration statistics to exported metrics
- authorization with cluster cookie is banned
- fix issue that could lead to deadlock in several TDG subsystems
- fix a several task/job issues
- ban "null" type for graphql
- fix array assign in updates
- empty filters handling in queries
- expired tuples are not returned anymore
- fix issues with ldap subsystem
- new option "use_active_directory" for ldap
- "organizational_units" option for ldap

## [2.2.0] - 2021-09-29

- support "ilike" - case-insensitive like
- like/ilike allowed only for string fields and explicitly banned for indexes
- tracing supports inheritance
- backward iteration without cursor is banned
- fixes some bugs with multitenancy
- add metrics for REST data interface
- added GraphQL interface for locking config sections to prevent section deletion ([#1213](https://github.com/tarantool/tdg2/issues/1213))
- "namespace" field in model is banned
- model and expiration graphql API were replaced with common data_type API
- added GraphQL interface for metrics settings ([#1221](https://github.com/tarantool/tdg2/issues/1221))

## [2.1.1] - 2021-09-02

- fixed the case when expiration has been defined, but the model doesn't ([#1184](https://github.com/tarantool/tdg2/issues/1184))
- fixed nullability array elements processing ([#1197](https://github.com/tarantool/tdg2/issues/1197))
- fixed multipart indexes with 2+ logicalTypes ([#1202](https://github.com/tarantool/tdg2/issues/1202))
- added validation for enums during updates. ([#228](https://github.com/tarantool/tdg2/issues/228))
- added input_processor.storage.type validation ([#1186](https://github.com/tarantool/tdg2/issues/1186))
- fixed validation of absent array elements ([#1204](https://github.com/tarantool/tdg2/issues/1204))
- banned same names of different connectors in the configuration
- allowed to setup more than one kafka input ([tdg#778](https://github.com/tarantool/tdg/issues/778))
- issues_limits parameter processed by cartridge, now ([#1174](https://github.com/tarantool/tdg2/issues/1174))

## [2.1.0] - 2021-08-24

- Initial public release of TDG2.
Use the documentation to get information about all the features.
