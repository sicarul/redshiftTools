context("internal.R")

test_that("%||% y if x is null", {
  x <- NULL
  y <- 2
  expect_equal(x %||% y, y)
})

test_that("%||% x if x is not null", {
  x <- 1
  y <- NULL
  expect_equal(x %||% y, x)
})

test_that("%||% x if x is not null and y is null", {
  x <- 1
  y <- NULL
  expect_equal(x %||% y, x)
})

test_that("%||% y if x is null", {
  x <- NULL
  y <- 2
  expect_equal(x %||% y, y)
})

test_that("%||% y if x is length 0", {
  x <- vector("numeric", 0)
  y <- 2
  expect_equal(x %||% y, y)
})

test_that("%||% x if x is not length 0", {
  x <- 1
  y <- vector("numeric", 0)
  expect_equal(x %||% y, x)
})

test_that("%||% x if x is not null and y is length 0", {
  x <- 1
  y <- vector("numeric", 0)
  expect_equal(x %||% y, x)
})
