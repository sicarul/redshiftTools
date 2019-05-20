## Test environments
* local OS X install, R 3.4.3
* ubuntu 12.04 (on travis-ci), R 3.4.3
* win-builder (devel and release)

## R CMD check results

0 errors | 0 warnings | 1 note

* This is a new release.

## Reverse dependencies

There are no reverse dependencies.

---

* Examples are all set as {dontrun} because they require a connection to an actual Amazon Redshift database and S3 buckets to work, otherwise they'd fail every time you run a check.

* Updating to fix an issue with excessive parallelization.
