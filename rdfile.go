package main

import "os"

// File Information struct
type fileInfo struct {
	filename   string
	fileSize   uint64
	start, end uint64
}

func (fileReader *fileInfo) info() {
	file, err := os.Open(fileReader.filename)

	if validate(err) {
		fileReader.fileSize = getfileSize(file)
		logger.Infof("[%s] fileSize: %d bytes", fileReader.filename, fileReader.fileSize)
		file.Close()
	} else {
		logger.Alert(" Cannot Open File:", fileReader.filename)
		logger.Critical("Closing application...")
		panic("Need fileSize to distribute load to workers")
	}
}

func (fileReader *fileInfo) read() []byte {
	file, err := os.Open(fileReader.filename)
	defer file.Close()

	if validate(err) {
		fileReader.fileSize = getfileSize(file)

		if fileReader.start < fileReader.end && fileReader.end <= fileReader.fileSize {
			offset := int(fileReader.end - fileReader.start + 1)
			_, err := file.Seek(int64(fileReader.start), 0)

			if validate(err) {
				var data = make([]byte, offset)
				logger.Debugf(" Reading %d bytes from %s ", offset, fileReader.filename)
				file.Read(data)
				return data
			}
			logger.Errorf("Unable to read file using seek values [%d,%d] ", fileReader.start, fileReader.end)

		} else if fileReader.start == 0 && fileReader.end == 0 {
			var data = make([]byte, fileReader.fileSize)
			logger.Debugf(" Reading whole file %s ", fileReader.filename)
			file.Read(data)
			return data
		} else {
			logger.Errorf("Invalid Seek options to read file [%d,%d] ", fileReader.start, fileReader.end)
		}
	} else {
		logger.Error("cannot open file: ", fileReader.filename)
		panic("Error..  Will now exit application ")
	}
	logger.Warningf("[%s] NO Bytes to return ", fileReader.filename)
	return make([]byte, 0)
}

func getfileSize(file *os.File) uint64 {
	stat, err := file.Stat()
	if validate(err) {
		return uint64(stat.Size())
	}
	logger.Alert("need to provide fileSize")
	logger.Critical("Application Error")
	panic("distribute workload")
}
