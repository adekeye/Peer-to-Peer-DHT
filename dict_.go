package main

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/vaughan0/go-zmq"
)

type myDict map[string](*node)

var (
	dictMutex = sync.RWMutex{}
)

func (gd myDict) add(iden string, nptr *node) {
	gd[iden] = nptr
}

func (gd myDict) contains(iden string) bool {
	j := gd[iden]

	if j == nil {
		return false
	}
	return true
}

func (gd myDict) remove(iden string) bool {
	dictMutex.RLock()
	defer dictMutex.RUnlock()

	if gd.contains(iden) {
		delete(gd, iden)
		return true
	}
	return false
}

func (gd myDict) get(iden string) **zmq.Channels {
	dictMutex.RLock()
	defer dictMutex.RUnlock()

	if gd.contains(iden) {
		return &gd[iden].channel
	}
	return nil
}

func (gd myDict) getRandomKey() string {
	dictMutex.RLock()
	defer dictMutex.RUnlock()

	lenDict := len(gd)

	if lenDict > 0 {

		a := make([]string, lenDict)

		i := 0

		for k := range gd {
			if gd[k].currState != initialization {
				a[i] = k
				i++
			}
		} //end of loop

		rand.Seed(time.Now().Unix())
		rVal := rand.Intn(len(a))
		return a[rVal]

	}
	logger.Info("dict size should be > 0 ")
	return ""
}

func (gd myDict) getSuccessor(key string) string {
	dictMutex.RLock()
	defer dictMutex.RUnlock()

	ky := IntParse(key)
	limit := int(powOfTwo(chordRing.RingSize))
	id := ky + 1

	for id != ky {
		indx := strconv.Itoa(id)

		if gd[indx] != nil {
			break
		}
		id = (id + 1) % limit
	}
	return strconv.Itoa(id)
}

func (gd myDict) getPredecessor(key string) string {
	dictMutex.RLock()
	defer dictMutex.RUnlock()
	ky := IntParse(key)
	limit := int(powOfTwo(chordRing.RingSize))
	pred := -1
	id := ky + 1

	for id != ky {
		indx := strconv.Itoa(id)

		if gd[indx] != nil {
			pred = id
		}
		id = (id + 1) % limit
	}
	return strconv.Itoa(pred)
}
