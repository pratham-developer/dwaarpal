package scripts

import _ "embed"

//go:embed fixed_window.lua
var FixedWindow string

//go:embed sliding_window.lua
var SlidingWindow string

//go:embed token_bucket.lua
var TokenBucket string
