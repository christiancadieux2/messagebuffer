package messagebuffer

import (
	"encoding/json"
	"os"

	"github.com/sumatra/analytics/util"
)

// hosts : kafka brokers comma separated list
// FileMaxTime  : max time(sec) before writing messages on a new file
//                  or processing new files.
// FileMaxSize  : max size(MB) before starting on a new file
// TotalSize    : max size of all files before pruning (MB)
// BufferDir    : directory for files
// PruneFrequency: in minutes

// Config config file.
type Config struct {
	FileMaxTime    int    `json:"file_max_time"`
	FileMaxSize    int    `json:"file_max_size"`
	TotalSize      int    `json:"total_size"`
	BufferDir      string `json:"buffer_dir"`
	PruneFrequency int    `json:"prune_frequency"`
}

func ReadConfig(cname string) (Config, error) {
	_, err := os.Stat(cname)
	var config Config
	if err != nil {
		util.Logln("Config file is missing: ", cname)
		return config, err
	}
	file, err := os.Open(cname)
	if err != nil {
		util.Logln("Cannot open config", err)
	}
	defer file.Close()
	jsonParser := json.NewDecoder(file)
	if err = jsonParser.Decode(&config); err != nil {
		util.Logln("error decoding ", err)
		return config, err
	}
	util.Logln("config", config)
	return config, nil
}
