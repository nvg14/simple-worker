package main

import (
	"log"
	"time"
)

type Action func()

type Job struct {
	JobId  int
	Action func()
}

type Status struct {
	Type   string
	ID     int
	Status string
}

type Worker struct {
	WorkerId       int
	jobs           chan *Job
	transmitStatus chan *Status
	Completed      chan bool
}

type Manager struct {
	counter     int
	jobQueue    chan *Job
	status      chan *Status
	workQueue   chan *Job
	workerQueue chan *Worker
}

func NewWorker(id int, workerQueue chan *Worker, jobQueue chan *Job, status chan *Status) *Worker {
	w := &Worker{
		WorkerId:       id,
		jobs:           jobQueue,
		transmitStatus: status,
	}

	go func() { workerQueue <- w }()
	return w
}

func NewManager() *Manager {
	d := &Manager{
		counter:     0,
		jobQueue:    make(chan *Job),
		status:      make(chan *Status),
		workQueue:   make(chan *Job),
		workerQueue: make(chan *Worker),
	}
	return d
}

func (w *Worker) Start() {
	go func() {
		for {
			select {
			case job := <-w.jobs:
				log.Printf("WorkerID : %d | JobID : %d", w.WorkerId, job.JobId)
				job.Action()
				w.transmitStatus <- &Status{Type: "worker", ID: w.WorkerId, Status: "completed"}
				w.Completed <- true
			case <-w.Completed:
				return
			}
		}
	}()
}

func (m *Manager) Start(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		worker := NewWorker(i, m.workerQueue, m.workQueue, m.status)
		worker.Start()
	}

	go func() {
		for {
			select {
			case job := <-m.jobQueue:
				log.Printf("Send job %d to work queue", job.JobId)
				m.workQueue <- job
			case ts := <-m.status:
				log.Printf("JobID : %d | Status : %s", ts.ID, ts.Status)
				if ts.Type == "worker" {
					if ts.Status == "completed" {
						m.counter--
					}
				}
			}
		}
	}()
}

func (m *Manager) AddJob(action Action) {
	j := &Job{
		JobId:  m.counter,
		Action: action,
	}

	go func() {
		m.jobQueue <- j
	}()

	m.counter++
}

func (m *Manager) Finished() bool {
	if m.counter < 1 {
		return true
	}
	return false
}

func createJob(jobID string, duration int) func() {
	return func() {
		log.Println("Starting Job : " + jobID)
		time.Sleep(time.Duration(duration) * time.Second)
		log.Println("Ending Job : " + jobID)
	}
}

func main() {
	m := NewManager()

	m.AddJob(createJob("a", 2))
	m.AddJob(createJob("b", 2))
	m.AddJob(createJob("c", 2))

	m.Start(3)

	//Uncomment this to trigger deadlock
	// m.Start(2)

	for {
		if m.Finished() {
			log.Println("Finished all jobs")
			break
		}
	}
}
