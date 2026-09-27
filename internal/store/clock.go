package store

import "time"

// nowTime is a package-level clock seam: production uses wall clock, tests
// can freeze it to make lease-expiry behavior deterministic.
var nowTime = time.Now
