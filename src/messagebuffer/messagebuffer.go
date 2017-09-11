package messagebuffer

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"util"

	"github.com/Shopify/sarama"
)

const providerUp = "U"
const providerDown = "D"

const processedPrefix = "P"
const seekPrefix = "S"
const topicDelim = "\t"       // delimiter topic | message
const rowDelim = '\n'         // buffer row delimiter
const processFilesPeriod = 15 // wait seconds to process more files
var gotSignal bool = false

// MessageBufferHandle handles buffering to provider.
// a separate goroutine send the data to provider from the files
// new files are started when old/big enough
// old files are prunes when too many
// may need to write to topic-specific  files and use multiple goroutine to keep-up

type MessageBufferHandle struct {
	state                    string
	fakeError                bool
	currentBufferFile        *os.File
	currentBufferFilename    string
	currentBufferFileSize    int64 // size of current bufferfile
	currentBufferFileCreated int64

	config         *sarama.Config
	allDone        bool // no more files to process
	maxBuffer      int32
	providerDownTs int64
	providerUpTs   int64
	provider       Provider
	//messageBuffer *buffer.MessageBuffer
	bufferConfig   Config
	bufferSendChan chan int8
	lastPruneTime  int64
	fileMux        sync.Mutex
	outputDelay    int
}

// NewBuffer Create messagebuffer.
func NewBuffer(provider Provider, configFilename string) (*MessageBufferHandle, error) {
	kc := new(MessageBufferHandle)
	kc.provider = provider
	bufferConfig, err := ReadConfig(configFilename)
	if err != nil {
		util.Logln("Cannot read config", err)
	}

	kc.bufferConfig = bufferConfig

	kc.state = providerUp
	kc.allDone = false

	kc.bufferSendChan = make(chan int8, 100)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		util.Logln(sig)
		gotSignal = true
	}()

	go kc.processFiles()

	return kc, nil
}

// get list of files with 'processed' status
func (kc *MessageBufferHandle) dirList(path string) ([]string, error) {

	files, err := ioutil.ReadDir(path) // sorted
	var results []string
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		name := f.Name()
		if name[0:1] == processedPrefix {
			results = append(results, name)
		}
	}
	return results, nil
}

// read buffer directory file by file, send to provider
// runs in separate goroutine
// run every FileMaxTime seconds or when a new file is generated

func (kc *MessageBufferHandle) processFiles() error {

	util.Logln("processFiles: init")
	var fileList []string
	var err error

	fileList, err = kc.dirList(kc.bufferConfig.BufferDir)
	// process existing files
	if err == nil {
		kc.processFilesList(fileList)
	}

	for {
		if gotSignal || kc.allDone {
			break
		}
		select {
		case <-time.After(time.Second * time.Duration(kc.bufferConfig.FileMaxTime)):
			fileList, err = kc.dirList(kc.bufferConfig.BufferDir)

		case <-kc.bufferSendChan:
			for len(kc.bufferSendChan) > 0 { // empty the channel
				<-kc.bufferSendChan
			}
			fileList, err = kc.dirList(kc.bufferConfig.BufferDir)
		}
		if err != nil {
			util.Logln("Cannot read", kc.bufferConfig.BufferDir, err)
			continue
		}
		kc.processFilesList(fileList)

		if (util.GetNow() - kc.lastPruneTime) > int64(60*kc.bufferConfig.PruneFrequency) {
			kc.lastPruneTime = util.GetNow()
			kc.pruneOldFiles() // remove if too old
		}
	}
	util.Logln("processFiles: all done!")
	return nil
}

// processFilesList create producers and process each file
func (kc *MessageBufferHandle) processFilesList(fileList []string) error {

	for _, name := range fileList {
		err := kc.provider.OpenProducer()
		if err != nil {
			util.Logln("Cannot create NewSyncProducer", err)
			return nil
		} else {
			util.Logln(" open producer")
		}
		start := time.Now()
		keepFile, rowCnt := kc.processOneFile(name)
		util.Logln("    ", util.Speed(rowCnt, start, kc.provider.Name()))

		if !keepFile {
			fullname := path.Join(kc.bufferConfig.BufferDir, name)
			util.Logln(" removing", fullname)
			os.Remove(fullname)
		}
		kc.provider.CloseProducer()
	}
	return nil
}

// send one file to provider , wait on provider error,
// give up when too long

func (kc *MessageBufferHandle) processOneFile(name string) (bool, int64) {

	dirPath := kc.bufferConfig.BufferDir
	fullname := path.Join(dirPath, name)
	util.Logln("  processOneFile:", fullname)
	file, err := os.Open(fullname)
	fPosStart := getSeek(kc.bufferConfig.BufferDir, name)
	if fPosStart > 0 {
		file.Seek(fPosStart, os.SEEK_SET)
		setSeek(dirPath, name, 0)
	}

	if err != nil {
		util.Logln("Cannot open", fullname)
		return true, 0
	}
	defer file.Close()
	scanner := bufio.NewReader(file)
	var fPos int64
	keepFile := false
	var rowCnt int64
	for {
		var line0 []byte
		var readErr error
		if gotSignal {
			setSeek(dirPath, name, fPos)
			keepFile = true
			break
		}
		if kc.outputDelay > 0 {
			time.Sleep(time.Duration(kc.outputDelay) * time.Microsecond)
		}

		line0, readErr = scanner.ReadBytes(rowDelim)
		if readErr != nil {
			break
		}
		line := string(line0[:len(line0)-1]) // remove rowDelim

		ix := strings.Index(line, topicDelim)
		if ix < 0 {
			util.Logln("Invalid line", line)
			continue
		}
		topic := string(line[0:ix])
		mess := string(line[ix+1:])

		_, err := kc.provider.SendMessage(topic, mess)
		rowCnt++
		if err == nil && !kc.fakeError {
			//util.Logln("sending ", len(mess), "bytes to provider"+kc.provider.Name())
			fPos += int64(len(line))

		} else {
			if kc.fakeError {
				kc.fakeError = false
				util.Logln("producer.SendMessage failed: fakeError")
			} else {
				util.Logln("producer.SendMessage failed", err)
			}
			setSeek(dirPath, name, fPos)
			kc.setDown()
			retryStart := util.GetNow()
			// wait less than PruneFrequency to avoid old messageFiles to pileup.
			pf := int64(kc.bufferConfig.PruneFrequency * 60 * 3 / 4)
			for util.GetNow()-retryStart < pf {
				time.Sleep(time.Duration(kc.provider.GetRetryWaitTime()) * time.Second)
				util.Logln("retrying provider..")
				_, err = kc.provider.SendMessage(topic, mess) // partition, offset
				if err == nil {
					kc.setUp()
					setSeek(dirPath, name, 0)
					break
				} else {
					util.Logln("producer.SendMessage failedAgain", err)
				}
			}
			if kc.state == providerDown {
				keepFile = true // retry file later at seek value
			}
		}
	}

	return keepFile, rowCnt
}

// save seek Value in 'S' file to reuse if needed when service restart

func setSeek(dirPath string, name string, fPos int64) {
	fullname := path.Join(dirPath, seekPrefix+name)
	if fPos == 0 {
		os.Remove(fullname)
	} else {
		file, _ := os.OpenFile(fullname, os.O_WRONLY|os.O_CREATE, 0666)
		file.WriteString(strconv.FormatInt(fPos, 10))
		file.Close()
	}
}

// get seek value from 'S' file
func getSeek(dirPath string, name string) int64 {
	fullname := path.Join(dirPath, seekPrefix+name)
	s, err := ioutil.ReadFile(fullname)
	if err != nil {
		return int64(0)
	}
	s2 := string(s)
	sPos, _ := strconv.ParseInt(s2, 10, 64)
	return sPos

}

// prune files that are too old regularly, runs on a schedule
// Starts from the last file and start removing after TotalSize MB...

func (kc *MessageBufferHandle) pruneOldFiles() error {

	files, err := ioutil.ReadDir(kc.bufferConfig.BufferDir)
	if err != nil {
		util.Logln("Cannot readDir", kc.bufferConfig.BufferDir)
		return nil
	}

	var totalSize int64
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		name := f.Name()
		if name[0:1] == processedPrefix {
			fi, err := os.Stat(path.Join(kc.bufferConfig.BufferDir, name))
			if err == nil {
				if totalSize > int64(kc.bufferConfig.TotalSize)*1000000 {
					os.Remove(path.Join(kc.bufferConfig.BufferDir, name)) // fail is ok
				} else {
					totalSize += fi.Size()
				}
			} else {
				util.Logln("Cannot Stat", name)
			}
		}
	}

	return nil
}

// for testing provider down
func (kc *MessageBufferHandle) InjectError() {
	kc.fakeError = true
}

func (kc *MessageBufferHandle) setDown() {
	kc.state = providerDown
	kc.providerDownTs = util.GetNow()
}

func (kc *MessageBufferHandle) setUp() {
	kc.state = providerUp
	kc.providerUpTs = util.GetNow()
}

// Close : close connection.
func (kc *MessageBufferHandle) Close() error {
	kc.closeRenameFile()
	kc.allDone = true
	return nil
	//return kc.provider.CloseProducer()

}

// SetOutputDelay saves millisec to wait between calls to kafka
func (kc *MessageBufferHandle) SetOutputDelay(s int) {
	kc.outputDelay = s
}

func (kc *MessageBufferHandle) newFileName() string {
	t := time.Now()
	t1 := fmt.Sprintf("%d%02d%02d-%02d%02d%02d", t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
	fn := path.Join(kc.bufferConfig.BufferDir, t1)
	util.Logln("-  generating file", fn)
	return fn
}

func (kc *MessageBufferHandle) closeRenameFile() error {
	if kc.currentBufferFile != nil {
		kc.currentBufferFile.Close()
		ix := strings.LastIndex(kc.currentBufferFilename, "/")
		newfile := path.Join(kc.currentBufferFilename[0:ix],
			processedPrefix+kc.currentBufferFilename[ix+1:])
		os.Rename(kc.currentBufferFilename, newfile)
		kc.bufferSendChan <- 1
	}
	return nil
}

// return current file descriptor
// or a new one if current file is old enough or big enough.

func (kc *MessageBufferHandle) getCurrentFile() (*os.File, error) {

	needNew := false

	if kc.currentBufferFile != nil {
		if kc.currentBufferFileSize > int64(kc.bufferConfig.FileMaxSize*1000000) { // too big, get a new one
			needNew = true
			util.Logln("neednew: too big")
		} else if kc.currentBufferFileSize > 0 {
			age := util.GetNow() - kc.currentBufferFileCreated

			if age > int64(kc.bufferConfig.FileMaxTime) { // too old, get new one
				util.Logln("neednew: age=", age, "max=", kc.bufferConfig.FileMaxTime)
				needNew = true
			}
		}
	} else {
		needNew = true
	}

	if needNew {
		kc.closeRenameFile()
		util.Logln("creating new file")
		filename := kc.newFileName()

		file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			util.Logln("Cannot open", filename)
			return nil, err
		}
		kc.currentBufferFile = file
		kc.currentBufferFilename = filename
		kc.currentBufferFileCreated = util.GetNow()
		kc.currentBufferFileSize = 0
	}
	return kc.currentBufferFile, nil
}

// WriteMessage : public method. (topic, message, [key])
func (kc *MessageBufferHandle) WriteMessage(topic string, message string, _ string) error {
	return kc.bufferMessage(topic, message)
}

//
func (kc *MessageBufferHandle) bufferMessage(topic string, message string) error {
	kc.fileMux.Lock()
	defer kc.fileMux.Unlock()
	f, _ := kc.getCurrentFile()
	//util.Logln("name=", kc.currentBufferFilename)
	message2 := strings.Replace(message, "\n", "\\n", -1) // in case
	m := topic + topicDelim + message2 + string(rowDelim)
	if _, err := f.WriteString(m); err != nil {
		util.Logln("Cannot write message ")
		return err
	}
	kc.currentBufferFileSize += int64(len(m))
	return nil
}
