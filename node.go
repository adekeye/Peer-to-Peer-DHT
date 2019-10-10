package main

import (
	"math"
	"strconv"
	"strings"

	"github.com/vaughan0/go-zmq"
)

type node struct {
	isRunning bool
	predID    string
	currState State
	fingerTab *fingerTable

	fingersReceived int
	channel         *zmq.Channels
	channelID       string
}

var (
	gdict *myDict
	temp  [][]byte
)

func (n *node) start(ptr *myDict) {
	gdict = ptr
	n.fingerTab = &fingerTable{parentNodeID: n.channelID}
	n.currState = initialization
	n.fingersReceived = 0
	n.fingerTab.init()
	n.listen()
}
func (n *node) listen() {
	n.isRunning = true
	for n.isRunning {
		nchan := <-((*n).channel.In())
		json := string(nchan[0])

		if strings.Contains(json, "data") {

			hq := &hashQuery{}
			hq.unmarshal(json)

			switch hq.Do {
			case get:
				k := IntParse(hq.Data.Key)

				if n.hasKey(hq.Data.Key) {
					val := ringHashTab.get(k)
					if len(val) > 0 {
						logger.Infof("[%s] Value obtained: %s for Key %d ", n.channelID, val, k)
						r := &response{Data: val, StateDetail: dataResponse}
						n.sendTo(hq.RespondTo, r.marshal())
					} else {
						logger.Infof("[%s] Value not obtained %d ", n.channelID, k)
					}
				} else {
					id := n.fingerTab.lookup(k)
					n.sendTo(id, json)
				}
				break
			case remove:
				k := IntParse(hq.Data.Key)
				if n.hasKey(hq.Data.Key) {
					val := ringHashTab.get(k)
					logger.Infof("[%s] Value removed: %s for Key %d ", n.channelID, val, k)
					ringHashTab.remove(k)
				} else {
					id := n.fingerTab.lookup(k)
					n.sendTo(id, json)
				}
				break
			case put:
				k := IntParse(hq.Data.Key)
				if n.hasKey(hq.Data.Key) {
					logger.Infof("[%s] Key-Value added: %d-%s ", n.channelID, k, hq.Data.Value)
					ringHashTab.put(k, hq.Data.Value)
				} else {
					id := n.fingerTab.lookup(k)
					n.sendTo(id, json)
				}
				break
			default:
				logger.Warning("[" + n.channelID + "] Hash Query not defined yet")
			}
		} else if strings.Contains(json, "respond-to") {

			rq := &QueryRing{}
			rq.unmarshal(json)

			switch rq.Do {

			case notify:
				logger.Info("[" + n.channelID + "] In ring notify ")
				if n.notify(rq.RespondTo) {
					n.fixFingers()
				}
				break
			case getFinger:
				if gdict.contains(rq.RespondTo) {
					if n.currState == initialization {
						logger.Info("[" + n.channelID + "] starting up.... Response cannot be granted to requests ")

					} else {
						r := &response{StateDetail: rq.StateDetail}
						r.Data = n.fingerTab.marshal()
						n.sendResponseTo(rq.RespondTo, r)
					}
				} else {
					logger.Warning("[" + n.channelID + "] Responder not found in myDict ")
				}
				break
			case findSuc:
				res := n.findSuccessor(rq.targetID, rq.RespondTo)
				if len(res) > 0 {
					r := &response{Data: res, Target: rq.targetID, StateDetail: rq.StateDetail}
					n.sendResponseTo(rq.RespondTo, r)
				} else {
					logger.Warningf("[%s] Succesor Not Obtainable (%s): %s ", n.channelID, rq.targetID, res)
				}

				break
			case findPre:
				res := n.findPredecessor(rq.targetID, rq.RespondTo)

				if len(res) > 0 {
					r := &response{Data: res, Target: rq.targetID, StateDetail: rq.StateDetail}
					logger.Debugf("[%s] Send find-predecessorMsg to %s | %s ", n.channelID, rq.RespondTo, r.marshal())
					n.sendResponseTo(rq.RespondTo, r)
				} else {
					res = gdict.getPredecessor(n.channelID)
					r := &response{Data: res, Target: rq.targetID, StateDetail: rq.StateDetail}
					logger.Debugf("[%s] Send find-predecessorMsg to %s | %s ", n.channelID, rq.RespondTo, r.marshal())
					n.sendResponseTo(rq.RespondTo, r)
				}

				break
			default:
				logger.Warning("[" + n.channelID + "] Ring Query not defined yet")

			}

		} else if strings.Contains(json, "state") {

			r := &response{}
			r.unmarshal(json)

			logger.Debug("[" + n.channelID + "] Response received " + json)

			switch r.StateDetail {

			case populateSuccResp:
				n.initGetSuccessorFingerTable(r)
				break
			case populateTableResp:
				n.initFillFingerTable(r)
				break
			case fixFingerResp:
				n.addFinger(r)
				break
			case stabilizeResp:
				n.stabilizeSetNewPredecessor(r)
				break
			case dataResponse:
				logger.Infof("[%s] Data Response received: %s", n.channelID, r.Data)
			default:
				logger.Infof("[%s] Unknown response: %s", n.channelID, json)

			}

		} else {

			a := &action{}
			a.unmarshal(json)
			switch a.Do {
			case join:
				logger.Info("[" + n.channelID + "] In Join Ring")
				fallthrough
			case initRing:
				logger.Info("[" + n.channelID + "] starting ring fingers ")
				n.populateFingerTable(a.Sponsor)
				break
			case stabilize:
				logger.Info("[" + n.channelID + "]  Ring stabilization in progress..")
				n.stabilize()
				break
			case fixRing:
				logger.Info("[" + n.channelID + "]  Fixing ring fingers ")
				n.fixFingers()
				break
			case leave:
				logger.Info("[" + n.channelID + "]  In Leave Ring")
				n.leaveRing(a)
				logger.Info("[" + n.channelID + "]  Ring exit!")
				break
			default:
				logger.Warning("Action not defined yet : " + json)
			}
		}
		json = ""
	}
}

func (n *node) populateFingerTable(sponsor string) {

	if gdict.contains(sponsor) {
		rq := &QueryRing{Do: findSuc, targetID: n.channelID, RespondTo: n.channelID, StateDetail: populateSuccResp}
		sChannel := *(gdict.get(sponsor))
		temp = [][]byte{[]byte(rq.marshal())}
		sChannel.Out() <- temp
		logger.Info("[" + n.channelID + "] Waiting for succesor Msg ")
		n.changeStateTo(populateSuccResp)

	} else if len(*gdict) <= 1 {
		logger.Info("[" + n.channelID + "] Cannot Find any sponsor .. I will Assign myself as the first Node ")
		n.predID = n.channelID

		for i := 0; i < chordRing.RingSize; i++ {
			n.fingerTab.add(i, n.channelID)
		}
		n.changeStateTo(none)
		n.fingerTab.print()

	} else {
		sponsor := gdict.getRandomKey()
		logger.Infof("[%s] Sponsor to be chosen at Random  -> %s ", n.channelID, sponsor)
		n.populateFingerTable(sponsor)
	}
}

func (n *node) initGetSuccessorFingerTable(r *response) {

	succResp := r.Data
	n.fingerTab.add(0, succResp)

	succChannel := *(gdict.get(succResp))
	rq := &QueryRing{Do: getFinger, RespondTo: n.channelID, StateDetail: populateTableResp}
	temp = [][]byte{[]byte(rq.marshal())}
	succChannel.Out() <- temp
	n.changeStateTo(populateTableResp)
	if n.channelID != succResp {
		logger.Infof("[%s] NotifyingNewSuccessorMsg: %s ", n.channelID, succResp)
		rq := &QueryRing{Do: notify, RespondTo: n.channelID}
		temp = [][]byte{[]byte(rq.marshal())}
		succChannel.Out() <- temp
	}

}

func (n *node) initFillFingerTable(r *response) {

	succResp := n.fingerTab.Ftable[0]

	fingTableJSON := r.Data
	ft := &fingerTable{}
	ft.unmarshal(fingTableJSON)

	logger.Infof("[%s] Received table: %v | Populated my finger table ", n.channelID, ft.Ftable)

	myid := IntParse(n.channelID)
	sid := IntParse(succResp)
	totalNodes := int(powOfTwo(chordRing.RingSize))
	pow := 2
	for i := 1; i < n.fingerTab.size(); i++ {
		id := (myid + pow) % totalNodes
		if id < sid {
			n.fingerTab.add(i, succResp)
		} else {
			val := gdict.getSuccessor(strconv.Itoa(id))
			n.fingerTab.add(i, val)
		}

		pow = pow * 2

	}
	n.changeStateTo(none)
	n.fingerTab.print()
}

func (n *node) leaveRing(act *action) {
	switch act.Mode {
	case orderly:

		gdict.remove(n.channelID)

		succ := n.fingerTab.Ftable[0]

		if len(succ) == 0 {
			succ = gdict.getSuccessor(n.channelID)
		}
		sChan := *gdict.get(succ)

		rq := &QueryRing{Do: notify, RespondTo: n.predID}
		temp = [][]byte{[]byte(rq.marshal())}
		sChan.Out() <- temp

		n.isRunning = false
		break
	case immediate:
		gdict.remove(n.channelID)
		n.isRunning = false
		break
	}
}

func (n *node) findSuccessor(targetID string, respondTo string) string {
	logger.Infof("[%s] FindSuccesorM | requested by: %s | target: %s ", n.channelID, respondTo, targetID)

	for i := 0; i < 5; i++ {

		succ := n.fingerTab.Ftable[i]
		if succ != n.channelID && gdict.contains(succ) {
			nid := IntParse(n.channelID)
			tid := IntParse(targetID)
			sid := IntParse(succ)
			if targetID == n.channelID {
				return succ
			} else if tid < sid && tid > nid {
				return succ
			} else {
				mySucc := gdict.getSuccessor(targetID)
				logger.Debugf("[%s] Successor(%s) found: %s", n.channelID, targetID, mySucc)
				return mySucc
			}
		}
	}
	return n.channelID
}

func (n *node) findPredecessor(targetID string, respondTo string) string {
	logger.Debug("[" + n.channelID + "]  InsidepredecessorM | target = " + targetID)

	if targetID == n.channelID {
		return n.predID
	}

	for i := 0; i < 5; i++ {
		succ := n.fingerTab.Ftable[i]
		if succ != n.channelID && gdict.contains(succ) {
			if succ == targetID {
				return n.channelID
			}
			break
		}
	}
	return n.channelID
}

func (n *node) fixFingers() {
	pow := 1
	myid := IntParse(n.channelID)
	totalNodes := int(powOfTwo(chordRing.RingSize))

	for i := 0; i < n.fingerTab.size(); i++ {
		rNodeID := gdict.getRandomKey()
		tid := strconv.Itoa(int(myid+pow) % totalNodes)
		rq := &QueryRing{Do: findSuc, targetID: tid, RespondTo: n.channelID, StateDetail: fixFingerResp}

		rChannel := *(gdict.get(rNodeID))
		temp = [][]byte{[]byte(rq.marshal())}
		rChannel.Out() <- temp

		n.changeStateTo(fixFingerResp)
		pow = pow * 2
	}
}

func (n *node) addFinger(r *response) {
	mid := IntParse(n.channelID)
	tid := IntParse(r.Target)

	if tid < mid {
		tid = int(powOfTwo(chordRing.RingSize)) + tid
	}

	logger.Debugf(" [%s] AddFingerMsg | Id: %d ", n.channelID, tid)

	diff := tid - mid
	index := int(math.Log2(float64(diff)))
	logger.Infof("[%s]  ReceivedFingerMsg for %d | Adding to index %d ", n.channelID, tid, index)

	v := gdict.getSuccessor(r.Target)
	n.fingerTab.add(index, v)
	n.fingersReceived++

	if n.fingersReceived == chordRing.RingSize {
		logger.Infof("[%s]FingersreceivedMsg | Predecessor: %s | New Table: %v ", n.channelID, n.predID, n.fingerTab.Ftable)
		n.fingersReceived = 0
		n.changeStateTo(none)
	}
}

func (n *node) notify(respondTo string) bool {
	sid := IntParse(n.fingerTab.Ftable[0])
	mid := IntParse(n.channelID)
	pid := IntParse(n.predID)
	rid := IntParse(respondTo)

	logger.Infof("[%d]  Current Predecessor Id: %d | Checking for: %d", mid, pid, rid)

	if len(n.predID) == 0 {
		logger.Infof("[%s] Predecessor updated to: %s ", n.channelID, respondTo)
		n.predID = respondTo
		return true
	} else if mid == pid {
		logger.Infof("[%s] Predecessor updated to: %s ", n.channelID, respondTo)
		n.predID = respondTo
		return true
	} else if gdict.contains(n.predID) == false {
		logger.Infof("[%s] Current Predecessor not found | New Updated to: %s ", n.channelID, respondTo)
		n.predID = respondTo
		return true
	} else if sid > mid && pid < mid {

		if rid < mid && rid > pid {
			logger.Infof("[%s] PredecessorupdatedMsg to: %s ", n.channelID, respondTo)
			n.predID = respondTo
			return true
		}
		logger.Infof("[%s] NotificationMsg %d ignored ", n.channelID, rid)

	} else if pid > mid && sid > mid && sid < pid {

		if pid < rid || mid > rid {
			logger.Infof("[%s] PredecessorupdatedMsg to: %s ", n.channelID, respondTo)
			n.predID = respondTo
			return true
		}
		logger.Infof("[%s] Notification ignored from %d", n.channelID, rid)

	} else if pid < mid && sid < mid && sid < pid {

		if rid < mid && rid > sid {
			logger.Infof("[%s] PredecessorupdatedMsg to: %s ", n.channelID, respondTo)
			n.predID = respondTo
			return true
		}
		logger.Infof("[%s] Notification ignored from %d", n.channelID, rid)

	} else if pid == sid {
		logger.Infof("[%s] PredecessorupdatedMsg to: %s ", n.channelID, respondTo)
		n.predID = respondTo
		return true

	} else {
		logger.Infof("[%s] Notification dicarded from %d", n.channelID, rid)
	}
	return false
}

func (n *node) stabilize() {
	succ := n.fingerTab.Ftable[0]

	if len(succ) > 0 && succ != n.channelID {

		rq := &QueryRing{Do: findPre, targetID: succ, RespondTo: n.channelID, StateDetail: stabilizeResp}

		if gdict.contains(succ) {

			rjson := rq.marshal()
			logger.Infof("[%s]  Notify-> find-predecessor(%s) requested : %s ", n.channelID, succ, rjson)
			sChannel := *(gdict.get(succ))
			//sChannel <- rjson
			temp = [][]byte{[]byte(rjson)}
			sChannel.Out() <- temp

			// Predecessor of my successor
			n.changeStateTo(stabilizeResp)

		} else {
			logger.Infof(" [%s] Notify-> find-predecessor(%s) request failed | Node not found ", n.channelID, succ)
			logger.Infof(" [%s] Fixing fingers... ", n.channelID)
			n.fingerTab.Ftable[0] = gdict.getSuccessor(n.channelID)
			n.fixFingers()
		}

	} else if succ == n.channelID {
		logger.Infof(" [%s] Solo Node detection... ", n.channelID)
		n.predID = gdict.getPredecessor(n.channelID)
		succ = gdict.getSuccessor(n.channelID)

		if n.fingerTab.Ftable[0] != succ {
			n.fingerTab.add(0, succ)
			n.fixFingers()

			if n.channelID != succ {
				logger.Infof("[%s] Notifying new successor: %s ", n.channelID, succ)
				rq := &QueryRing{Do: notify, RespondTo: n.channelID}
				chanID := *(gdict.get(succ))
				temp = [][]byte{[]byte(rq.marshal())}
				chanID.Out() <- temp
			}
		}
	}
}

// Predecessor of my successor
func (n *node) stabilizeSetNewPredecessor(r *response) {
	succ := n.fingerTab.Ftable[0]
	predOfSucc := r.Data

	sid := IntParse(succ)
	mid := IntParse(n.channelID)
	posid := IntParse(predOfSucc)
	update := false

	if posid == mid {

		logger.Infof("[%s] This is Predecessor of Successor ", n.channelID)
		n.notify(succ)

	} else if sid == posid {

		logger.Infof("[%s] Successor&Predecessor of Successor is same ", n.channelID)
		n.notify(succ)

	} else if n.checkValidSuccessor(posid) {

		logger.Infof("[%s] Successor updated %s ", n.channelID, predOfSucc)
		n.fingerTab.add(0, predOfSucc)
		update = true

	} else {
		logger.Infof("[%s] SuccessornotupdatedMsg. ", n.channelID)
		logger.Debugf("[%d] Successor Id: %d | Pred Of Succ: %d", mid, sid, posid)
	}

	n.changeStateTo(none)

	if update == true {

		rq := &QueryRing{Do: notify, RespondTo: n.channelID}
		nsucc := n.fingerTab.Ftable[0]
		sChannelPtr := gdict.get(nsucc)

		if sChannelPtr != nil {
			sChannel := *sChannelPtr
			temp = [][]byte{[]byte(rq.marshal())}
			sChannel.Out() <- temp
		} else {
			logger.Errorf("[%s] Notify(%s) failed! ", n.channelID, nsucc)
		}

	}
	n.fingerTab.print()
}

func (n *node) sendTo(nodeid string, data string) bool {
	if gdict.contains(nodeid) {
		rchannel := *(gdict.get(nodeid))
		//rchannel <- data
		temp = [][]byte{[]byte(data)}
		rchannel.Out() <- temp
		return true
	}
	logger.Warning("[" + n.channelID + "] Node " + nodeid + " NOT found in global dict" + data)
	return false
}

func (n *node) sendResponseTo(nodeid string, r *response) bool {
	if gdict.contains(nodeid) {
		rchannel := *(gdict.get(nodeid))
		//rchannel <- r.marshal()
		temp = [][]byte{[]byte(r.marshal())}
		rchannel.Out() <- temp
		return true
	}
	logger.Warning("[" + n.channelID + "] Node " + nodeid + " NOT found in global dict ")
	return false
}

func (n *node) hasKey(key string) bool {
	if len(key) > 0 {
		mid := IntParse(n.channelID)
		pid := IntParse(n.predID)
		k := IntParse(key)
		total := int(powOfTwo(chordRing.RingSize))

		if pid > mid {
			if k > pid && k < total {
				return true
			} else if k >= 0 && k <= mid {
				return true
			} else {
				return false
			}
		} else if k > pid && k <= mid {
			return true
		} else {
			return false
		}
	} else {
		logger.Infof(" [%s] Invalid key Length", n.channelID)
		return false
	}
}

func (n *node) changeStateTo(currState State) {
	logger.Debugf(" [%s] Changing state from %d to %d ", n.channelID, n.currState, currState)
	n.currState = currState
}

func (n *node) checkValidSuccessor(id int) bool {
	mid := IntParse(n.channelID)
	sid := IntParse(n.fingerTab.Ftable[0])

	if sid > mid {

		if id < sid && id > mid {
			return true
		}
		return false

	}
	if id > sid && id < mid {
		return false
	}
	return true
}
