package scripts

import _ "embed"

//go:embed fixed_window.lua
var FixedWindow string

//go:embed sliding_window_log.lua
var SlidingWindowLog string

//go:embed token_bucket.lua
var TokenBucket string

//go:embed sliding_window_counter.lua
var SlidingWindowCounter string

//go:embed leaky_bucket.lua
var LeakyBucket string

// Cached SHA hashes
var (
	FixedWindowSHA          string
	SlidingWindowLogSHA     string
	TokenBucketSHA          string
	SlidingWindowCounterSHA string
	LeakyBucketSHA          string
)
