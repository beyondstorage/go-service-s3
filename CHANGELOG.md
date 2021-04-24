# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [v1.2.0] - 2021-04-24

### Added

- *: Add UnimplementedStub (#61)
- tests: Introduce STORAGE_S3_INTEGRATION_TEST (#65)
- storage: Add configurations and support SSE (#63)
- storage: Implement AOS-40 (#68)

### Changed

- ci: Only run Integration Test while push to master
- storage: Update SSE pair description (#66)

### Upgraded

- build(deps): bump github.com/aws/aws-sdk-go from 1.38.19 to 1.38.23 (#64)
- build(deps): bump github.com/aws/aws-sdk-go from 1.38.23 to 1.38.24 (#67)

## [v1.1.0] - 2021-04-14

### Added

- *: Implement default pair support for service (#16)
- storage: Implement Create (#56)

### Changed

- build(deps): bump github.com/aws/aws-sdk-go to 1.38.19 (#57)

## v1.0.0 - 2021-02-07

### Added

- Implement s3 services.

[v1.2.0]: https://github.com/aos-dev/go-service-s3/compare/v1.1.0...v1.2.0
[v1.1.0]: https://github.com/aos-dev/go-service-s3/compare/v1.0.0...v1.1.0
