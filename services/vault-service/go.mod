module coordos/vault-service

go 1.22

require (
	coordos/vml-core v0.0.0
	coordos/project-core v0.0.0
	coordos/resolver v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require github.com/lib/pq v1.11.2 // indirect

replace (
	coordos/vml-core => ../../packages/vml-core
	coordos/project-core => ../../packages/project-core
	coordos/resolver => ../../packages/resolver
	coordos/vuri => ../../packages/vuri
)
