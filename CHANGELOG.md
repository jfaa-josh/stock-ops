# Changelog

## [0.12.11](https://github.com/jfaa-josh/stock-ops/compare/v0.12.10...v0.12.11) (2026-02-10)


### Bug Fixes

* **ci2:** Line endings problem fix ([#60](https://github.com/jfaa-josh/stock-ops/issues/60)) ([af19b7b](https://github.com/jfaa-josh/stock-ops/commit/af19b7b38369999117f85648f523b84ac45ecc81))

## [0.12.10](https://github.com/jfaa-josh/stock-ops/compare/v0.12.9...v0.12.10) (2026-02-10)


### Bug Fixes

* **ci:** Re-order tests in ci to avoid a possible race conditions which caused a ci fail in test_data.txt processing (shape returned 1 row short of input dataset) ([#58](https://github.com/jfaa-josh/stock-ops/issues/58)) ([091ca20](https://github.com/jfaa-josh/stock-ops/commit/091ca208bfb50b0e5afb4d7cb56657f0015c85d6))

## [0.12.9](https://github.com/jfaa-josh/stock-ops/compare/v0.12.8...v0.12.9) (2026-02-10)


### Bug Fixes

* **housekeeping:** Fix/housekeeping ([#56](https://github.com/jfaa-josh/stock-ops/issues/56)) ([5a6db5c](https://github.com/jfaa-josh/stock-ops/commit/5a6db5c530d5ce0d8026029af1d1e4cbcf4d057a))

## [0.12.8](https://github.com/jfaa-josh/stock-ops/compare/v0.12.7...v0.12.8) (2026-02-06)


### Bug Fixes

* **nginx-entrypoint-bug:** Fixing bug in nginx-entrypoint.sh that tried to set empty env vars. ([#54](https://github.com/jfaa-josh/stock-ops/issues/54)) ([8c702ae](https://github.com/jfaa-josh/stock-ops/commit/8c702ae6a8c4bb57628dea2f3887f86de5a2012d))

## [0.12.7](https://github.com/jfaa-josh/stock-ops/compare/v0.12.6...v0.12.7) (2026-02-06)


### Bug Fixes

* **nginx-entrypoint.sh:** Implemented nginx-entrypoint.sh to build conf templates and wired to dockercompose ([#52](https://github.com/jfaa-josh/stock-ops/issues/52)) ([a9c7530](https://github.com/jfaa-josh/stock-ops/commit/a9c753026aadaa5347eeaf1eb2b80f8de4e9222f))

## [0.12.6](https://github.com/jfaa-josh/stock-ops/compare/v0.12.5...v0.12.6) (2026-02-06)


### Bug Fixes

* **conf:** Fix to require local to use stockops.conf.template so that prod and local each access their own respective conf file. ([#50](https://github.com/jfaa-josh/stock-ops/issues/50)) ([a63bf23](https://github.com/jfaa-josh/stock-ops/commit/a63bf23c97d763a92389b8b4c7c73045cc537443))

## [0.12.5](https://github.com/jfaa-josh/stock-ops/compare/v0.12.4...v0.12.5) (2026-02-04)


### Bug Fixes

* **prod:** htpasswd check ([#48](https://github.com/jfaa-josh/stock-ops/issues/48)) ([59c9c96](https://github.com/jfaa-josh/stock-ops/commit/59c9c96b75e61403ba62d7805b98fdebf3479c03))

## [0.12.4](https://github.com/jfaa-josh/stock-ops/compare/v0.12.3...v0.12.4) (2026-02-04)


### Bug Fixes

* **release trigger:** trigger release pipeline for last PR ([#46](https://github.com/jfaa-josh/stock-ops/issues/46)) ([f134007](https://github.com/jfaa-josh/stock-ops/commit/f1340074a25c41a67487a48025456a7fd3b77b01))

## [0.12.3](https://github.com/jfaa-josh/stock-ops/compare/v0.12.2...v0.12.3) (2026-02-04)


### Bug Fixes

* **prodauth:** Fix/release cd bugs ([#43](https://github.com/jfaa-josh/stock-ops/issues/43)) ([eda88b5](https://github.com/jfaa-josh/stock-ops/commit/eda88b51d21ba039b5fd9218de16d181a76be06a))

## [0.12.2](https://github.com/jfaa-josh/stock-ops/compare/v0.12.1...v0.12.2) (2026-01-30)


### Bug Fixes

* **cd:** Added profiles to docker compose build call for services in cd.yml that need to be released so that docker has access. ([#41](https://github.com/jfaa-josh/stock-ops/issues/41)) ([5ffd4ec](https://github.com/jfaa-josh/stock-ops/commit/5ffd4ec2cf61059310b84d617deddf8517c1d0ee))

## [0.12.1](https://github.com/jfaa-josh/stock-ops/compare/v0.12.0...v0.12.1) (2026-01-28)


### Bug Fixes

* **nginx:** containerized nginx, certs, and htpass services using custom images for push to GHCR. Modified releases to include stockops.sh entrypoint and .env.example. Modified readme instructions. ([#39](https://github.com/jfaa-josh/stock-ops/issues/39)) ([8907e09](https://github.com/jfaa-josh/stock-ops/commit/8907e0995bf81d73bce80b7436e3896a679491db))

## [0.12.0](https://github.com/jfaa-josh/stock-ops/compare/v0.11.0...v0.12.0) (2026-01-27)


### Features

* **nginx:** add secure public and local deployment ([#37](https://github.com/jfaa-josh/stock-ops/issues/37)) ([2d9e6d2](https://github.com/jfaa-josh/stock-ops/commit/2d9e6d255e53045f90b352b8c454d3f3dfc8a31c))

## [0.11.0](https://github.com/jfaa-josh/stock-ops/compare/v0.10.0...v0.11.0) (2025-09-23)


### Features

* **cd:** Feat/cd refinements ([#34](https://github.com/jfaa-josh/stock-ops/issues/34)) ([4624eaa](https://github.com/jfaa-josh/stock-ops/commit/4624eaa03301e40bb717109e2610c8dfa306078a))

## [0.10.0](https://github.com/jfaa-josh/stock-ops/compare/v0.9.0...v0.10.0) (2025-09-19)


### Features

* **cd:** Trying to fix exit from loop due to error ([#32](https://github.com/jfaa-josh/stock-ops/issues/32)) ([b814dbc](https://github.com/jfaa-josh/stock-ops/commit/b814dbc9a30016196fa3d546969382405096a11a))

## [0.9.0](https://github.com/jfaa-josh/stock-ops/compare/v0.8.0...v0.9.0) (2025-09-19)


### Features

* **cd:** Second attempt to modify custom build image names sourced correctly for tagging an push step ([#30](https://github.com/jfaa-josh/stock-ops/issues/30)) ([5bdadab](https://github.com/jfaa-josh/stock-ops/commit/5bdadab40ec5fc1d03ef70f56758b637f8e7d4d4))

## [0.8.0](https://github.com/jfaa-josh/stock-ops/compare/v0.7.0...v0.8.0) (2025-09-19)


### Features

* **cd:** modify Tag & push app images to verify correct image names ([#28](https://github.com/jfaa-josh/stock-ops/issues/28)) ([0604a07](https://github.com/jfaa-josh/stock-ops/commit/0604a07bd3d5b6e84d79b4c19fbd063cd3769324))

## [0.7.0](https://github.com/jfaa-josh/stock-ops/compare/v0.6.0...v0.7.0) (2025-09-18)


### Features

* **cd:** Feat/fixing ghcr nopush ([#26](https://github.com/jfaa-josh/stock-ops/issues/26)) ([5f65368](https://github.com/jfaa-josh/stock-ops/commit/5f6536892acd0c9bd539ad767f8208cd3edf6b5a))

## [0.6.0](https://github.com/jfaa-josh/stock-ops/compare/v0.5.0...v0.6.0) (2025-09-18)


### Features

* **cd:** Tested and verified new exec for Pin compose to release tag using yq ([#23](https://github.com/jfaa-josh/stock-ops/issues/23)) ([bb0d5cd](https://github.com/jfaa-josh/stock-ops/commit/bb0d5cd7ce823ec380496afd81cddc8e6c526586))

## [0.5.0](https://github.com/jfaa-josh/stock-ops/compare/v0.4.0...v0.5.0) (2025-09-18)


### Features

* **cd:** Fixing parsing on yq for cd ([#21](https://github.com/jfaa-josh/stock-ops/issues/21)) ([f44ebbd](https://github.com/jfaa-josh/stock-ops/commit/f44ebbdc64f3c1a14716b650335f90b7d954b1b2))

## [0.4.0](https://github.com/jfaa-josh/stock-ops/compare/v0.3.0...v0.4.0) (2025-09-18)


### Features

* **cd:** Fixing incorrect flags on yq for cd ([#19](https://github.com/jfaa-josh/stock-ops/issues/19)) ([8cd6a1c](https://github.com/jfaa-josh/stock-ops/commit/8cd6a1ce87d653a078b6b3f79f1925a36530804f))

## [0.3.0](https://github.com/jfaa-josh/stock-ops/compare/v0.2.0...v0.3.0) (2025-09-18)


### Features

* **cd:** Feature/fix ci cd ([#17](https://github.com/jfaa-josh/stock-ops/issues/17)) ([a9635e4](https://github.com/jfaa-josh/stock-ops/commit/a9635e408063696cd11c5d82dfad239a8329a314))

## [0.2.0](https://github.com/jfaa-josh/stock-ops/compare/v0.1.0...v0.2.0) (2025-09-17)


### Features

* prepare release ([f0c8cad](https://github.com/jfaa-josh/stock-ops/commit/f0c8cad89ca1f3d62c43a26551822033868eb603))

## 0.1.0 (2025-09-17)


### Features

* bootstrap initial release ([#10](https://github.com/jfaa-josh/stock-ops/issues/10)) ([5f7c1d1](https://github.com/jfaa-josh/stock-ops/commit/5f7c1d15ea92a591ea44b762584d5e03de283881))
* bootstrap initial release ([#9](https://github.com/jfaa-josh/stock-ops/issues/9)) ([12b8487](https://github.com/jfaa-josh/stock-ops/commit/12b8487a685623670ea99f0ab7028414c0f09962))
