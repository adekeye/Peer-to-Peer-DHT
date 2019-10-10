package main

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type table []string

type fingerTable struct {
	parentNodeID string
	Ftable       table `json:"table"`
	Length       int   `json:"Length"`
}

func (ft *fingerTable) init() {
	ft.Length = int(chordRing.RingSize)
	ft.Ftable = make(table, ft.Length)
}

func (ft *fingerTable) add(index int, value string) bool {
	if index >= 0 && index < ft.Length {
		ft.Ftable[index] = value
		logger.Debugf("["+ft.parentNodeID+"] Added to FT | Index: %d | Value: %s ", index, value)
		return true
	}
	logger.Errorf("["+ft.parentNodeID+"] Add Invalid Index to ft | Size: %d | Index given: %d", ft.Length, index)
	return false
}

func (ft *fingerTable) lookup(key int) string {
	i := 0
	logger.Debugf("["+ft.parentNodeID+"] Searching for ", key, " with ", ft.Ftable)

	for i < ft.Length {
		n := IntParse(ft.Ftable[i])
		if int(key) < n {
			return ft.Ftable[i]
		}
		i++
	}

	l := len(ft.Ftable)
	if l > 0 {
		return ft.Ftable[l-1]
	}
	return ft.Ftable[0]
}

func (ft *fingerTable) delete(index int) bool {
	if index >= 0 && index < ft.Length {
		ft.Ftable[index] = ""

		return true
	}
	logger.Errorf("["+ft.parentNodeID+"] remove index from Fingertable | Size: %d | Index given: %d", ft.Length, index)
	return false
}

func (ft *fingerTable) size() int {
	return len(ft.Ftable)
}

func (ft *fingerTable) print() {
	var buffer bytes.Buffer
	logger.Info(" Finger Table of " + ft.parentNodeID + " is")
	buffer.WriteString("-----------\n")

	for i := 0; i < ft.size(); i++ {
		t := fmt.Sprintf("| %d | %s |\n", (i + 1), ft.Ftable[i])
		buffer.WriteString(t)
	}

	buffer.WriteString("-----------")
	fmt.Println(buffer.String())
}

func (ft *fingerTable) marshal() string {
	bytarr, err := json.Marshal(*ft)
	if validate(err) {
		return string(bytarr)
	}
	panic(err)
}
func (ft *fingerTable) unmarshal(jsonStr string) {
	json.Unmarshal([]byte(jsonStr), ft)
}

func powOfTwo(exp int) int64 {
	var res int64 = 1
	for i := 1; i <= exp; i++ {
		res *= 2
	}
	return res
}
