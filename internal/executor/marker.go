//go:build ignore

// Marker binary used by executor tests: sleeps forever as a uniquely
// named process so process-group reaping can be checked with pgrep -x.
package main

import "time"

func main() { time.Sleep(1 * time.Hour) }
