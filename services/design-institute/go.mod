module coordos/design-institute

go 1.22

require (
	coordos/project-core v0.0.0
	coordos/resolver v0.0.0
	coordos/vuri v0.0.0
	github.com/lib/pq v1.10.9
	gopkg.in/yaml.v3 v3.0.1
)

replace (
	coordos/project-core => ../../packages/project-core
	coordos/resolver => ../../packages/resolver
	coordos/vuri => ../../packages/vuri
)
