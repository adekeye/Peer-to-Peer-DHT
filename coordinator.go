package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vaughan0/go-zmq"
)

// Sock is a zeromq socket struct
type Sock struct {
	ctx   *zmq.Context
	sock  *zmq.Socket
	chans *zmq.Channels
}

func makeSocket() Sock {
	newChans := sock.Channels()
	return Sock{ctx: ctx, sock: sock, chans: newChans}
}

type coordinator struct {
	liveChanges []liveConfig
	gDict       myDict
	ctx         *zmq.Context
	sock        *zmq.Socket
}

var (
	stabilizelock      sync.WaitGroup
	initializationlock sync.WaitGroup
	chanGroup          []Sock
	ringHashTab        hashtab
)

func (cd *coordinator) start() {
	cd.gDict = make(myDict)
	ringSize := powOfTwo(int(chordRing.RingSize))
	chanGroup = make([]Sock, ringSize)
	ringHashTab = make([]string, ringSize)
	liveChanges := chordRing.LiveChanges

	sponsor := ""
	cd.initiateJoin(chordRing.Node, sponsor)

	time.Sleep(time.Duration(2500 * 1000000))
	go cd.sendStabilize()

	for i := 0; i < len(liveChanges); i++ {

		timetoWait := liveChanges[i].Time * 1000000
		actionTodo := liveChanges[i].Action
		query := liveChanges[i].Query
		nodeIP := liveChanges[i].NodeID

		if len(query) > 0 {
			cd.sendQuery(query, liveChanges[i].Data, nodeIP, timetoWait)
		} else {
			cd.sendAction(actionTodo, nodeIP, timetoWait)
		}
	}
}

func (cd *coordinator) initiateJoin(NodeID string, sponsorID string) *node {
	index := consistentHashing(NodeID)
	chanGroup[index] = makeSocket()
	cp := chanGroup[index].chans

	channelID := fmt.Sprintf("%d", index)
	//logger.Debug(" Join to node initiaition " + channelID)
	logger.Infof(" [%s] Mapped to %s with sponsor %s", NodeID, channelID, sponsorID)
	if cd.gDict.contains(channelID) == false {

		act := &action{Do: join, Sponsor: sponsorID}
		n := &node{channel: cp, channelID: channelID}
		cd.gDict.add(channelID, n)
		s := act.marshal()

		logger.Info(" [" + channelID + "] Starting Node ")
		go n.start(&cd.gDict)
		//logger.Infof(" Sending action %s to %s ", s, channelID)
		temp = [][]byte{[]byte(s)}
		(*n).channel.Out() <- temp
		return n
	}
	logger.Debug(" Node already exists, to initiate ")
	return nil
}

func (cd *coordinator) sendAction(actionTODO NodeAction, nodeIP string, waitTime uint64) {
	logger.Debug(" Time wait ")
	time.Sleep(time.Duration(waitTime))
	logger.Debug("Time wait ended!")

	if actionTODO == join {
		sponsor := cd.gDict.getRandomKey()
		cd.initiateJoin(nodeIP, sponsor)
	} else {

		chanID := consistentHashing(nodeIP)
		act := &action{Do: actionTODO}
		cd.updateAction(act, chanID)
		s := act.marshal()

		ch := gdict.get(strconv.Itoa(chanID))

		if ch != nil {
			logger.Info("Sending action: ", s)
			nChan := *ch
			temp = [][]byte{[]byte(s)}
			nChan.Out() <- temp
		} else {
			logger.Errorf("Node %s not found to send action %s", nodeIP, actionTODO)
		}
	}
}

func (cd *coordinator) updateAction(act *action, chanID int) {
	switch act.Do {

	case join:
		sponsor := cd.gDict.getRandomKey()
		act.Sponsor = sponsor
	case leave:
		rValue := rand.Intn(2)
		if rValue == 0 {
			act.Mode = orderly
		} else {
			act.Mode = immediate
		}
		break
	}

}

func (cd *coordinator) sendQuery(query HashQuery, keyValue string, nodeIP string, waitTime uint64) {
	logger.Debug("Time wait ")
	time.Sleep(time.Duration(waitTime))
	logger.Debug("Time wait ended!")
	chanID := consistentHashing(nodeIP)
	nid := strconv.Itoa(chanID)
	arr := strings.Split(keyValue, "=")
	d := &data{}

	if len(arr) == 1 {
		d.Key = arr[0]
	} else if len(arr) > 1 {
		d.Key = arr[0]
		d.Value = arr[1]
	} else {
		logger.Error("No Data to perform hash query")
		return
	}

	act := &hashQuery{Do: query, Data: d, RespondTo: nid}
	s := act.marshal()

	ch := gdict.get(nid)

	if ch != nil {
		logger.Info("Sending query: ", s)
		nChan := *ch
		temp = [][]byte{[]byte(s)}
		nChan.Out() <- temp
	} else {
		logger.Errorf("Node %s[%s] not found to send {query:%s}", nodeIP, nid, query)
	}
}

func (cd *coordinator) sendStabilize() {
	stabilizelock.Add(1)

	for true {
		time.Sleep(time.Duration(chordRing.StabilizePeriod * 1000000))

		for k := range cd.gDict {
			logger.Info("Sending Stabilize to " + k)
			a := &action{Do: stabilize}
			channel := *(cd.gDict.get(k))
			temp = [][]byte{[]byte(a.marshal())}
			channel.Out() <- temp
		}
	}
	stabilizelock.Done()
}
