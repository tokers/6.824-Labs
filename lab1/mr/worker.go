package mr

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

var (
	_pid int
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

type intermediate struct {
	filenames []string
	job       *Job
}

func init() {
	_pid = os.Getpid()
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func unmarshal(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	var data []KeyValue
	for {
		var kv KeyValue
		_, err := fmt.Fscanf(file, "%s %s\n", &kv.Key, &kv.Value)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(filename, ": ", err)
		}
		data = append(data, kv)
	}
	sort.Sort(ByKey(data))
	return data
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := &worker{
		mapf:    mapf,
		reducef: reducef,
	}

	passCh := make(chan intermediate)
	feedbackCh := make(chan struct{})

	go w.doReduce(passCh, feedbackCh)
	w.doMap(passCh, feedbackCh)
}

func (w *worker) doMap(passCh chan intermediate, feedbackCh chan struct{}) {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:9999")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for {
		var j Job
		err = client.Call("JobDispatcher.Dispatch", _pid, &j)

		if err != nil {
			if err.Error() == _errNoJobTemp.Error() {
				time.Sleep(time.Second)
				continue
			}
			if err.Error() == _errDone.Error() {
				close(passCh)
				<-feedbackCh
				return
			}

			panic(err)
		}

		log.Printf("got job with filename: %s\n", j.Filename)

		file, err := os.Open(j.Filename)
		if err != nil {
			log.Fatal(err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal(err)
		}
		file.Close()
		kva := w.mapf(j.Filename, string(content))

		assignment := make(map[string]*bytes.Buffer)

		for _, kv := range kva {
			n := ihash(kv.Key) % j.Seed
			output := fmt.Sprintf("mr-%d-%d", j.ID, n)
			if _, ok := assignment[output]; !ok {
				assignment[output] = new(bytes.Buffer)
			}
			buf, _ := assignment[output]
			buf.WriteString(kv.Key)
			buf.WriteString(" ")
			buf.WriteString(kv.Value)
			buf.WriteString("\n")
		}

		var in intermediate

		for filename, buf := range assignment {
			// just truncate the file if it exists.
			if err := ioutil.WriteFile(filename, buf.Bytes(), 0644); err != nil {
				log.Fatal(err)
			}

			in.filenames = append(in.filenames, filename)
		}
		in.job = &j
		passCh <- in
		<-feedbackCh

		fmt.Println(j)
		if err := client.Call("JobDispatcher.Finish", &j, nil); err != nil {
			log.Fatal(err)
		}
	}
}

func (w *worker) doReduce(passCh chan intermediate, feedbackCh chan struct{}) {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:9999")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for {
		intermediate, ok := <-passCh
		if !ok {
			close(feedbackCh)
			return
		}
		var alldata []KeyValue

		for _, filename := range intermediate.filenames {
			data := unmarshal(filename)

			i := 0
			for i < len(data) {
				j := i + 1
				for j < len(data) && data[j].Key == data[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, data[k].Value)
				}
				output := w.reducef(data[i].Key, values)
				alldata = append(alldata, KeyValue{data[i].Key, output})
				i = j
			}
		}
		sort.Sort(ByKey(alldata))
		oname := fmt.Sprintf("mr-out-%d", intermediate.job.ID)
		ofile, _ := os.Create(oname)
		for _, kv := range alldata {
			fmt.Fprintf(ofile, "%s %s\n", kv.Key, kv.Value)
		}
		ofile.Close()

		feedbackCh <- struct{}{}
	}
}
