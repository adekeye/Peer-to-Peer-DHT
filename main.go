package main

import (
	log "github.com/alexcesaro/log/stdlog"
	"github.com/vaughan0/go-zmq"

	"strconv"
)

var (
	logger       = log.GetFromFlags()
	fileMetaData *fileInfo
	chordRing    *config
	ctx          *zmq.Context
	sock         *zmq.Socket
)

func validate(err error) bool {
	if err != nil {
		logger.Alert(err)
		return false
	}
	return true
}

// IntParse is a function that conducts error handling for converting strings to integers
func IntParse(str string) int {

	if len(str) == 0 {
		return int(0)
	}
	val, err := strconv.ParseInt(str, 10, 64)

	if err != nil {
		logger.Error("Cannot parse", str, ":", err)
		return int(val)
	}
	return int(val)
}

func main() {
	rdfile := "datafile"
	fileMetaData := &fileInfo{filename: rdfile}
	fbyte := fileMetaData.read()
	chordRing = &config{}
	chordRing.unmarshal(string(fbyte))

	newCtx, err := zmq.NewContext()
	if err != nil {
		panic(err)
	}
	newSock, err := newCtx.Socket(zmq.Rep)
	if err != nil {
		panic(err)
	}
	if err = newSock.Bind("tcp://*:621"); err != nil {
		panic(err)
	}

	m := coordinator{liveChanges: chordRing.LiveChanges, ctx: newCtx, sock: newSock}
	ctx = newCtx
	sock = newSock
	m.start()

	stabilizelock.Wait()
	logger.Close()
}
