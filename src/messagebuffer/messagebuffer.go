package messagebuffer

import (
	"bufio"
	"clog"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
	"util"

	"github.com/Shopify/sarama"
)

// status: Up | Down
const providerUp = "U"
const providerDown = "D"

// State: live | buffering
const stateLive = 1
const stateBuffering = 2
const blockingRetries = 10

const readyPrefix = "P" // ready to be send to provider
const seekPrefix = "S"
const topicDelim = "\t" // delimiter topic | message
const rowDelim = '\n'   // buffer row delimiter

const ModeBlockOnError = 1  // retry on error, block client
const ModeErrorOnError = 2  // return provider error to client
const ModeBufferOnError = 3 // start buffering on error, return success
const ModeAlwaysBuffer = 4  // always buffer first, goroutine handle provider

// MessageBufferHandle handles buffering to provider.
//  ref: https://github.com/elodina/go_kafka_client
// Headwaters: https://github.comcast.com/headwaters/headwaters-examples
// a separate goroutine send the data to provider from the files
// new files are started when old/big enough
// old files are prunes when too many
// may need to write to topic-specific  files and use multiple goroutine to keep-up

type MessageBufferHandle struct {
	state                    int
	providerStatus           string
	fakeError                bool
	currentBufferFile        *os.File
	errorMode                int
	currentBufferFilename    string
	currentBufferFileSize    int64 // size of current bufferfile
	currentBufferFileCreated time.Time

	config         *sarama.Config
	allDone        bool // no more files to process
	maxBuffer      int32
	providerDownTs time.Time
	providerUpTs   time.Time
	provider       Provider
	//messageBuffer *buffer.MessageBuffer
	bufferConfig   Config
	bufferSendChan chan int8
	lastPruneTime  time.Time
	fileMux        sync.Mutex
	outputDelay    int
	context        context.Context
	logger         *clog.Logger
}

// NewBuffer Create messagebuffer.
func NewBuffer(ctx context.Context, provider Provider, configFilename string,
	logger *clog.Logger, errorMode int) (*MessageBufferHandle, error) {
	kc := new(MessageBufferHandle)
	kc.provider = provider
	kc.logger = logger
	bufferConfig, err := ReadConfig(configFilename)

	if errorMode < ModeBlockOnError || errorMode > ModeErrorOnError {
		panic("errorMode is invalid!")
	}
	kc.errorMode = errorMode

	if err != nil {
		logger.Error("Cannot read config", err)
	}

	kc.bufferConfig = bufferConfig
	kc.state = stateLive
	kc.providerStatus = providerUp
	kc.allDone = false

	kc.bufferSendChan = make(chan int8, 100)

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
		if name[0:1] == readyPrefix {
			results = append(results, name)
		}
	}
	return results, nil
}

// read buffer directory file by file, send to provider
// runs in separate goroutine
// run every FileMaxTime seconds or when a new file is generated

func (kc *MessageBufferHandle) processFiles() error {

	kc.logger.Info("processFiles: init")
	var fileList []string
	var err error

	for {
		if kc.allDone {
			break
		}
		// get new list at fixed time interval or on bufferSendChan
		select {
		case <-kc.bufferSendChan:
			fileList, err = kc.dirList(kc.bufferConfig.BufferDir)

		case <-time.After(time.Second * time.Duration(kc.bufferConfig.FileMaxTime)):
			if kc.state == stateLive {
				break
			}
			fileList, err = kc.dirList(kc.bufferConfig.BufferDir)

		case <-kc.context.Done():
			kc.logger.Info("context.Done")
			break
		}
		if err != nil {
			kc.logger.Info("Cannot read", kc.bufferConfig.BufferDir, err)
			continue
		}
		kc.processFilesList(fileList)

		if time.Since(kc.lastPruneTime).Minutes() > float64(kc.bufferConfig.PruneFrequency) {
			kc.lastPruneTime = time.Now()
			kc.pruneOldFiles() // remove if too old
		}
	}
	kc.logger.Info("processFiles: all done!")
	return nil
}

// processFilesList create producers and process each file
func (kc *MessageBufferHandle) processFilesList(fileList []string) error {

	for _, name := range fileList {
		err := kc.provider.OpenProducer()
		if err != nil {
			kc.logger.Info("Cannot create NewSyncProducer", err)
			return nil
		} else {
			kc.logger.Info(" open producer")
		}
		start := time.Now()
		keepFile, rowCnt := kc.processOneFile(name)
		kc.logger.Info("    ", util.Speed(rowCnt, start, kc.provider.Name()))

		if !keepFile {
			fullname := path.Join(kc.bufferConfig.BufferDir, name)
			kc.logger.Info(" removing", fullname)
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
	kc.logger.Info("  processOneFile:", fullname)
	file, err := os.Open(fullname)
	fPosStart := getSeek(kc.bufferConfig.BufferDir, name)
	if fPosStart > 0 {
		file.Seek(fPosStart, os.SEEK_SET)
		setSeek(dirPath, name, 0)
	}

	if err != nil {
		kc.logger.Info("Cannot open", fullname)
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
			kc.logger.Info("Invalid line", line)
			continue
		}
		topic := string(line[0:ix])
		mess := string(line[ix+1:])

		ix2 := strings.Index(mess, topicDelim)
		key := string(line[0:ix2])
		mess = string(line[ix2+1:])

		_, err := kc.provider.SendMessage(topic, mess, key)
		rowCnt++
		if err == nil && !kc.fakeError {
			//kc.logger.Info("sending ", len(mess), "bytes to provider"+kc.provider.Name())
			fPos += int64(len(line))

		} else {
			if kc.fakeError {
				kc.fakeError = false
				kc.logger.Info("producer.SendMessage failed: fakeError")
			} else {
				kc.logger.Info("producer.SendMessage failed", err)
			}
			setSeek(dirPath, name, fPos)
			kc.setDown()
			retryStart := time.Now()
			// wait less than PruneFrequency to avoid old messageFiles to pileup.
			for time.Since(retryStart).Minutes() < float64(3/4*kc.bufferConfig.PruneFrequency) {
				time.Sleep(time.Duration(kc.provider.GetRetryWaitTime()) * time.Second)
				kc.logger.Info("retrying provider..")
				_, err = kc.provider.SendMessage(topic, mess, key) // return partition, offset
				if err == nil {
					kc.setUp()
					setSeek(dirPath, name, 0)
					break
				} else {
					kc.logger.Info("producer.SendMessage failedAgain", err)
				}
			}
			if kc.providerStatus == providerDown {
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
		kc.logger.Info("Cannot readDir", kc.bufferConfig.BufferDir)
		return nil
	}

	var totalSize int64
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		name := f.Name()
		if name[0:1] == readyPrefix {
			fi, err := os.Stat(path.Join(kc.bufferConfig.BufferDir, name))
			if err == nil {
				if totalSize > int64(kc.bufferConfig.TotalSize)*1000000 {
					os.Remove(path.Join(kc.bufferConfig.BufferDir, name)) // fail is ok
				} else {
					totalSize += fi.Size()
				}
			} else {
				kc.logger.Info("Cannot Stat", name)
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
	kc.providerStatus = providerDown
	kc.providerDownTs = time.Now()
}

func (kc *MessageBufferHandle) setUp() {
	kc.providerStatus = providerUp
	kc.providerUpTs = time.Now()
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
	kc.logger.Info("-  generating file", fn)
	return fn
}

func (kc *MessageBufferHandle) closeRenameFile() error {
	if kc.currentBufferFile != nil {
		kc.currentBufferFile.Close()
		ix := strings.LastIndex(kc.currentBufferFilename, "/")
		newfile := path.Join(kc.currentBufferFilename[0:ix],
			readyPrefix+kc.currentBufferFilename[ix+1:])
		os.Rename(kc.currentBufferFilename, newfile)
		if len(kc.bufferSendChan) < 10 {
			kc.bufferSendChan <- 1
		}
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
			kc.logger.Info("neednew: too big")
		} else if kc.currentBufferFileSize > 0 {
			age := time.Since(kc.currentBufferFileCreated).Seconds()

			if age > float64(kc.bufferConfig.FileMaxTime) { // too old, get new one
				kc.logger.Info("neednew: age=", age, "max=", kc.bufferConfig.FileMaxTime)
				needNew = true
			}
		}
	} else {
		needNew = true
	}

	if needNew {
		kc.closeRenameFile()
		kc.logger.Info("creating new file")
		filename := kc.newFileName()

		file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			kc.logger.Info("Cannot open", filename)
			return nil, err
		}
		kc.currentBufferFile = file
		kc.currentBufferFilename = filename
		kc.currentBufferFileCreated = time.Now()
		kc.currentBufferFileSize = 0
	}
	return kc.currentBufferFile, nil
}

// WriteMessage : public method. (topic, message, [key])
// Add an automatic way to start buffering when provider throughput is too low.
// Recovering from stateBuffering would require:
//   - the provider is now UP for a minimum duration and the number of
//      pending buffers is low.
//   - Close and write the current buffer.
//   - Wait for all pending buffers to be processed by other thread.
//   - Change the state to stateLive.
// before returning.

func (kc *MessageBufferHandle) WriteMessage(topic string, message string, key string) error {

	if kc.errorMode == ModeErrorOnError {
		_, err := kc.provider.SendMessage(topic, message, key)
		return err
	}

	if kc.errorMode == ModeBlockOnError {
		retryStart := time.Now()
		var err = errors.New("Provider timeout error")
		totalWait := kc.provider.GetRetryWaitTime() * blockingRetries

		for time.Since(retryStart).Seconds() < float64(totalWait) {
			time.Sleep(time.Duration(kc.provider.GetRetryWaitTime()) * time.Second)
			kc.logger.Info("retrying provider..")
			_, err1 := kc.provider.SendMessage(topic, message, key) // return partition, offset
			if err1 == nil {
				err = nil
				break
			} else {
				kc.logger.Info("producer.SendMessage failedAgain", err)
			}
		}
		return err
	}

	if kc.errorMode == ModeBufferOnError || kc.errorMode == ModeAlwaysBuffer {
		if kc.state == stateLive && kc.errorMode != ModeAlwaysBuffer {
			_, err := kc.provider.SendMessage(topic, message, key) // partition, offset
			if err == nil {
				return nil
			}
			kc.state = stateBuffering
			return kc.bufferMessage(topic, message, key)

		} else { // keep buffering after first provider error
			return kc.bufferMessage(topic, message, key)
		}
	}

	return nil
}

//
func (kc *MessageBufferHandle) bufferMessage(topic string, message string, key string) error {
	kc.fileMux.Lock()
	defer kc.fileMux.Unlock()
	f, _ := kc.getCurrentFile()
	//kc.logger.Info("name=", kc.currentBufferFilename)
	message2 := strings.Replace(message, "\n", "\\n", -1) // in case
	m := topic + topicDelim + key + topicDelim + message2 + string(rowDelim)
	if _, err := f.WriteString(m); err != nil {
		kc.logger.Info("Cannot write message ")
		return err
	}
	kc.currentBufferFileSize += int64(len(m))
	return nil
}
