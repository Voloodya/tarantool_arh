# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.5.0] - 2021-08-09

### Added

- Name and format of a space `_ddl_sharding_key` is a part of public API.

### Changed

- If one applies an empty schema using the 'ddl-manager' role API,
  put into the clusterwide configuration an example instead.

## [1.4.0] - 2021-04-22

### Added

- Use transactional ddl when applying schema.
- Transfer "ddl-manager" role from the cartridge repo.

## [1.3.0] - 2020-12-25

### Added

- Allow custom fields in space format.
- Forbid redundant keys in schema top-level and make `spaces` table
  mandatory. So the only valid schema format now is `{spaces = {...}}`.

## [1.2.0] - 2020-07-20

### Added

- Support `uuid` types for tarantool 2.4

## [1.1.0] - 2020-04-09

### Added

- Support `decimal` and `double` types for tarantool 2.3

### Fixed

- Remove unnecessary logs
- Fix error messages

## [1.0.0] - 2019-11-28

### Added

- Basic functionality
- Sharding key support
- Integration tests
- Luarock-based packaging
- Gitlab CI integration
