package mapreduce

import (
	//"hash/fnv"
    //"io/ioutil"
    "os"
    "io"
    //"fmt"
    //"strings"
    //"log"
    "encoding/json"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

    var intermediate_struct map[string][]string
    intermediate_struct = make(map[string][]string)

    for mapNumber := 0; mapNumber < nMap; mapNumber ++ {
        inFile := reduceName(jobName, mapNumber, reduceTask)
        file, err := os.Open(inFile)
        e_check(err)
        var tmpJson []KeyValue

        dec := json.NewDecoder(file)
    	for {
    		var tmp KeyValue
    		if err := dec.Decode(&tmp); err == io.EOF {
    			break
    		} else if err != nil {
    			panic(err)
    		}
    		//fmt.Printf("%s: %s\n", tmp.Key, tmp.Value)
            tmpJson = append(tmpJson, tmp)
    	}
        file.Close()

        //Write to giant json file
        for _, kv := range tmpJson {
            intermediate_struct[kv.Key] = append(intermediate_struct[kv.Key],kv.Value)
        }
    }
    //fmt.Println("Before reduceF")

    out, err := os.Create(outFile)
    //fmt.Println(outFile)
    e_check(err)
    enc := json.NewEncoder(out)
	for k,v := range intermediate_struct {
		enc.Encode(KeyValue{k, reduceF(k, v)})
        //fmt.Println("k ", k, " reduce: ", reduceF(k, v))
	}
	out.Close()


}
