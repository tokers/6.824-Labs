package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type jobState uint8

const (
	_jobPending = iota
	_jobInFlight
	_jobDone
)

var (
	_jobTimeout         = 10 * time.Second
	_errDone            = errors.New("all jobs finished")
	_errNoJobTemp       = errors.New("no job temporarily")
	_errDubiousJobState = errors.New("dubious job state")
)

type Job struct {
	Filename string
	Seed     int
	ID       int
}

type JobDispatcher struct {
	// TODO use a finer-granularity lock here.
	sync.Mutex
	rest         int
	done         bool
	jobChan      chan *Job
	timedoutChan chan *Job
	doneChan     chan struct{}
	jobState     map[string]jobState
	timedout     map[string]struct{}
	timers       map[string]*time.Timer
}

func (d *JobDispatcher) Dispatch(_ int64, j *Job) error {
	if d.done {
		return _errDone
	}
	select {
	case jj := <-d.jobChan:
		d.Lock()
		timer := time.AfterFunc(_jobTimeout, func() {
			d.timedoutChan <- j
		})
		d.timers[jj.Filename] = timer
		if _, ok := d.jobState[jj.Filename]; ok {
			panic("bad job")
		}
		d.jobState[jj.Filename] = _jobInFlight
		d.Unlock()
		*j = *jj
		return nil
	default: // no job
		return _errNoJobTemp
	}
}

func (d *JobDispatcher) Finish(j, _ *Job) error {
	d.Lock()
	defer d.Unlock()
	if d.rest <= 0 {
		panic("rest job must be a non-positive number")
	}

	if _, ok := d.timedout[j.Filename]; ok {
		// job was timed out.
		// timed out job was removed from jobState and resent to jobChan.
		delete(d.timedout, j.Filename)
		return nil
	}

	if s, ok := d.jobState[j.Filename]; !ok || s != _jobInFlight {
		return _errDubiousJobState
	} else {
		d.jobState[j.Filename] = _jobDone
		d.rest--
		if d.rest == 0 {
			close(d.doneChan)
		}
	}
	log.Println("job", j.Filename, "finished")

	if timer, ok := d.timers[j.Filename]; ok {
		timer.Stop()
		delete(d.timers, j.Filename)
	}
	return nil
}

type Master struct {
	done       bool
	dispatcher *JobDispatcher
}

// MakeMaster makes a master.
func MakeMaster(inputs []string, nReduce int) *Master {
	if len(inputs) == 0 {
		return nil
	}

	// removed the duplicate files.
	sort.Strings(inputs)
	files := []string{inputs[0]}

	for i := 1; i < len(inputs); i++ {
		if inputs[i] != inputs[i-1] {
			files = append(files, inputs[i])
		}
	}

	dispatcher := &JobDispatcher{
		rest:         len(inputs),
		timedoutChan: make(chan *Job),
		jobChan:      make(chan *Job),
		doneChan:     make(chan struct{}),
		jobState:     make(map[string]jobState),
		timedout:     make(map[string]struct{}),
		timers:       make(map[string]*time.Timer),
	}
	m := &Master{
		done:       false,
		dispatcher: dispatcher,
	}

	go m.dispatcher.run(files, nReduce)
	go m.run()
	return m
}

func (j *Job) reset(d *JobDispatcher) {
}

func (m *Master) run() {
	if err := rpc.Register(m.dispatcher); err != nil {
		panic(err)
	}
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		panic(err)
	}

	go http.Serve(listener, nil)
}

// Done() reports whether the entire job has finished, it's caller's
// responsibility to call it periodically.
func (m *Master) Done() bool {
	return m == nil || m.done
}

func (d *JobDispatcher) run(inputs []string, nReduce int) {
	go func() {
		for i, input := range inputs {
			j := &Job{Filename: input, Seed: nReduce, ID: i}
			d.jobChan <- j
		}
	}()

	for {
		select {
		case tjob := <-d.timedoutChan:
			log.Println("job: ", tjob.Filename, " timed out")
			d.Lock()
			delete(d.jobState, tjob.Filename)
			delete(d.timers, tjob.Filename)
			d.timedout[tjob.Filename] = struct{}{}
			d.redo(tjob)
			d.Unlock()
		case <-d.doneChan:
			d.Lock()
			d.done = true
			d.Unlock()
			return
		}
	}
}

func (d *JobDispatcher) redo(j *Job) {
	go func() {
		d.jobChan <- j
	}()
}
