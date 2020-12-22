package taskmanager

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("taskmanager")

type Task interface {
	Execute(context.Context) error
	Name() string
}

type TaskWithProgress interface {
	Pause()
	Progress() (float64, error)
	Description() string
	Name() string
}

type RestartableTask interface {
	Restart(context.Context)
}

type ErrorListener interface {
	HandleError()
}

type WorkerInfo struct {
	TaskName string
	Status   string
}

type TaskManager struct {
	taskQueue chan chan Task
	wmMutex   *sync.Mutex
	workerMap map[int32]*WorkerInfo
	tWPMutex  *sync.Mutex
	taskWPMap map[string]TaskWithProgress
	context   context.Context
	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	running   int32
	max       int32
}

func NewTaskManager(parentContext context.Context,
	maxWorkerCount int32) *TaskManager {
	ctx, cancel := context.WithCancel(parentContext)
	manager := &TaskManager{
		taskQueue: make(chan chan Task, maxWorkerCount+10),
		workerMap: make(map[int32]*WorkerInfo, maxWorkerCount),
		taskWPMap: map[string]TaskWithProgress{},
		context:   ctx,
		cancel:    cancel,
		wmMutex:   &sync.Mutex{},
		tWPMutex:  &sync.Mutex{},
		max:       maxWorkerCount,
		wg:        &sync.WaitGroup{},
	}
	manager.addWorkers(int(maxWorkerCount / 2))

	return manager
}

var workerId int32 = 0

func (m *TaskManager) addWorkers(count int) {
	m.wmMutex.Lock()
	defer m.wmMutex.Unlock()
	for i := 0; i < count; i++ {
		currentId := atomic.AddInt32(&workerId, 1)
		log.Infof("Starting worker : %d", currentId)
		m.workerMap[currentId] = &WorkerInfo{TaskName: "Idle", Status: "waiting"}
		w := newWorker(currentId, m)
		w.Start()
		_ = atomic.AddInt32(&m.running, 1)
		m.wg.Add(1)
	}
}

func (m *TaskManager) GoWork(newTask Task) {
	go func() {
		twp, ok := newTask.(TaskWithProgress)
		if ok {
			m.tWPMutex.Lock()
			_, exists := m.taskWPMap[newTask.Name()]
			m.tWPMutex.Unlock()
			if exists {
				log.Errorf("Trying to enqueue same task. Not allowed.")
				return
			} else {
				m.tWPMutex.Lock()
				m.taskWPMap[newTask.Name()] = twp
				m.tWPMutex.Unlock()
			}
		}
		for {
			select {
			case <-time.After(time.Millisecond * 500):
				if m.max > m.running {
					newWorkers := math.Ceil(float64(m.max-m.running) / 2)
					log.Infof("Adding %d new workers", int(newWorkers))
					m.addWorkers(int(newWorkers))
				}
			case worker := <-m.taskQueue:
				open := true
				select {
				case _, open = <-worker:
				default:
				}
				if !open {
					log.Warnf("Worker was timed out. Ignoring...")
					break
				}
				log.Infof("Dispatching task %s to worker", newTask.Name())
				worker <- newTask
				return
			case <-m.context.Done():
				return
			}
		}
	}()
}

func (m *TaskManager) updateState(wId int32, name, status string) {
	m.wmMutex.Lock()
	defer m.wmMutex.Unlock()
	m.workerMap[wId].TaskName = name
	m.workerMap[wId].Status = status
}

func (m *TaskManager) Status() map[int32]*WorkerInfo {
	m.wmMutex.Lock()
	defer m.wmMutex.Unlock()
	return m.workerMap
}

func (m *TaskManager) TaskWithProgressStatus() map[string]TaskWithProgress {
	m.tWPMutex.Lock()
	defer m.tWPMutex.Unlock()
	return m.taskWPMap
}

func (m *TaskManager) Stop() {
	log.Info("Got stop request")
	m.cancel()
	m.wg.Wait()
}

func isFatal(err error) bool {
	return false
}

func (m *TaskManager) handleStart(id int32, tName string) {
	log.Infof("Worker %d started running task %s", id, tName)
	m.updateState(id, tName, "running")
}

func (m *TaskManager) handleError(id int32, t Task, err error) {
	log.Infof("Worker %d had error %s running task %s", id, err.Error(), t.Name())
	m.updateState(id, "Idle", "waiting")
	if isFatal(err) {
		m.Stop()
	}
	if _, ok := t.(TaskWithProgress); ok {
		log.Infof("Found failed progress task. Cleaning up")
		m.tWPMutex.Lock()
		delete(m.taskWPMap, t.Name())
		m.tWPMutex.Unlock()
	}
	if r, ok := t.(RestartableTask); ok {
		log.Infof("Found restartable task. Restarting")
		<-time.After(time.Second * 5)
		r.Restart(m.context)
		m.GoWork(t)
		return
	}
	return
}

func (m *TaskManager) handleSuccess(id int32, t Task) {
	log.Infof("Worker %d successfully completed task %s", id, t.Name())
	m.updateState(id, "Idle", "waiting")
	if _, ok := t.(TaskWithProgress); ok {
		log.Infof("Found successful progress task. Cleaning up")
		m.tWPMutex.Lock()
		delete(m.taskWPMap, t.Name())
		m.tWPMutex.Unlock()
	}
}

func (m *TaskManager) handleWorkerTimeout(w *worker) {
	log.Infof("Timed out Worker %d", w.id)
	if m.max > (m.running * 2) {
		log.Info("Worker count going below threshold. Restarting worker")
		w.Start()
		return
	}
	m.handleWorkerStop(w)
}

func (m *TaskManager) handleWorkerStop(w *worker) {
	log.Infof("Removing Worker %d", w.id)
	m.wmMutex.Lock()
	defer m.wmMutex.Unlock()
	delete(m.workerMap, w.id)
	_ = atomic.AddInt32(&m.running, -1)
	m.wg.Done()
}
