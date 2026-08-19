module defra-finite-diffs-adapter

go 1.24.0

require (
	github.com/ahenzinger/finite-diffs-pir v0.0.0
	github.com/zeebo/blake3 v0.2.4
)

replace github.com/ahenzinger/finite-diffs-pir => ../..
