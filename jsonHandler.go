package main

import "encoding/json"

// -------------------------------------------------------------
// JSON Helper structs
// -------------------------------------------------------------

type jsonObject interface {
	marshal() string
	unmarshal(str string)
}

type action struct {
	Do      NodeAction `json:"do"`
	Sponsor string     `json:"sponsoring-node,omitempty"`
	Mode    ActionMode `json:"mode,omitempty"`
}

// QueryRing is a json msg struct for checking on the ring
type QueryRing struct {
	Do          NodeQuery `json:"do"`
	RespondTo   string    `json:"respond-to"`
	targetID    string    `json:"target-id,omitempty"`
	StateDetail State     `json:"state,omitempty"`
}

type hashQuery struct {
	Do        HashQuery `json:"do"`
	Data      *data     `json:"data"`
	RespondTo string    `json:"respond-to"`
}

type data struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type response struct {
	StateDetail State  `json:"state"`
	Data        string `json:"response"`
	Target      string `json:"target",omitempty`
}

type config struct {
	RingSize        int          `json:"ring.size"`
	Node            string       `json:"startup.node.id"`
	StabilizePeriod int64        `json:"stabilize.period.millis"`
	LiveChanges     []liveConfig `json:"liveChanges,omitempty"`
}
type liveConfig struct {
	NodeID string     `json:"id"`
	Time   uint64     `json:"timeInMillis"`
	Action NodeAction `json:"action,omitempty"`
	Query  HashQuery  `json:"query,omitempty"`
	Data   string     `json:"data,omitempty"`
}

// -------------------------------------------------------------
// JSON Helper funcions
// -------------------------------------------------------------

func (act *action) marshal() string {
	bytarr, err := json.Marshal(*act)
	if validate(err) {
		return string(bytarr)
	}
	panic(err)
}
func (act *action) unmarshal(jsonStr string) {
	json.Unmarshal([]byte(jsonStr), act)
}

func (rq *QueryRing) marshal() string {
	bytarr, err := json.Marshal(*rq)
	if validate(err) {
		return string(bytarr)
	}
	panic(err)
}
func (rq *QueryRing) unmarshal(jsonStr string) {
	json.Unmarshal([]byte(jsonStr), rq)
}

func (hq *hashQuery) marshal() string {
	bytarr, err := json.Marshal(*hq)
	if validate(err) {
		return string(bytarr)
	}
	panic(err)
}
func (hq *hashQuery) unmarshal(jsonStr string) {
	json.Unmarshal([]byte(jsonStr), hq)
}

func (dt *data) marshal() string {
	bytarr, err := json.Marshal(*dt)
	if validate(err) {
		return string(bytarr)
	}
	panic(err)
}
func (dt *data) unmarshal(jsonStr string) {
	json.Unmarshal([]byte(jsonStr), dt)
}

func (r *response) marshal() string {
	bytarr, err := json.Marshal(*r)
	if validate(err) {
		return string(bytarr)
	}
	panic(err)
}
func (r *response) unmarshal(jsonStr string) {
	json.Unmarshal([]byte(jsonStr), r)
}

func (cfg *config) marshal() string {
	bytarr, err := json.Marshal(*cfg)
	if validate(err) {
		return string(bytarr)
	}
	panic(err)
}
func (cfg *config) unmarshal(jsonStr string) {
	json.Unmarshal([]byte(jsonStr), cfg)
}

func (loc *liveConfig) marshal() string {
	bytarr, err := json.Marshal(*loc)
	if validate(err) {
		return string(bytarr)
	}
	panic(err)
}
func (loc *liveConfig) unmarshal(jsonStr string) {
	json.Unmarshal([]byte(jsonStr), loc)
}
