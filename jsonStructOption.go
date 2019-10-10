package main

// NodeAction is a struct of constant values for strings relating to possible actions done on nodes
type NodeAction string

const (
	initRing  NodeAction = "init-ring-fingers"
	fixRing   NodeAction = "fix-ring-fingers"
	leave     NodeAction = "leave-ring"
	join      NodeAction = "join-ring"
	stabilize NodeAction = "stabilize-ring"
)

// NodeQuery is a struct of constant values for strings relating to possible queries requested by nodes
type NodeQuery string

const (
	notify    NodeQuery = "ring-notify"
	getFinger NodeQuery = "get-ring-fingers"
	findSuc   NodeQuery = "find-ring-successor"
	findPre   NodeQuery = "find-ring-predecessor"
)

// State is a struct of constant values for the various states possible to occur in the ring
type State int

const (
	none              State = -1
	initialization          = 0
	stabilizeResp     State = 1
	fixFingerResp     State = 2
	populateSuccResp  State = 3
	populateTableResp State = 4
	dataResponse      State = 5
)

// HashQuery is a struct of constant values for strings relating to possible queries to make changes to the hash of a node
type HashQuery string

const (
	get    HashQuery = "get"
	put    HashQuery = "put"
	remove HashQuery = "remove"
)

// ActionMode is a struct of constant values for strings relating to whether an action is completed
// orderly (giving time to properly restructure the ring) or not (immediately exiting and restructuring after the fact)
type ActionMode string

const (
	immediate ActionMode = "immediate"
	orderly   ActionMode = "orderly"
)
