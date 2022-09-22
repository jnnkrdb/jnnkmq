package rabbitmq

import (
	"encoding/json"
	"os"

	"github.com/jnnkrdb/corerdb/prtcl"
)

// queue definition for the message queue
//
// can be inserted via json-string
//
//	{
//		"name": "",
//		"durable": true,
//		"autodelete": false,
//		"exclusiv": false,
//		"nowait": false
//	}
type QueueDefinition struct {
	Name       string `json:"name"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"autodelete"`
	Exclusiv   bool   `json:"exclusiv"`
	NoWait     bool   `json:"nowait"`
}

// load a configuration from a file
//
// Parameters:
//   - `path` : string > path to the jsonfile, which contains the settings
func LoadQueueDefintion(path string) (queue QueueDefinition, err error) {

	prtcl.Log.Println("loading rabbitmq-auth configuration from", path)

	if jsonf, err := os.ReadFile(path); err == nil {

		if err = json.Unmarshal(jsonf, &queue); err != nil {

			prtcl.PrintObject(jsonf, queue, err)
		}

	} else {

		prtcl.PrintObject(jsonf, queue, err)
	}

	return
}
