module coordos/resolver

go 1.21

require (
	coordos/vuri v0.0.0
	coordos/project-core v0.0.0
	github.com/lib/pq v1.10.9
)

replace (
	coordos/vuri => ../vuri
	coordos/project-core => ../project-core
)
