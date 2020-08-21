package taskmanager

import (
	"context"
	"time"
)

type worker struct {
	id        int32
	task      chan Task
	taskQueue chan chan Task
	context   context.Context
	// Manager callbacks to handle state
	onStart   func(int32, string)
	onError   func(int32, Task, error)
	onSuccess func(int32, Task)
	onTimeout func(*worker)
	onStopped func(*worker)
}

func newWorker(id int32, m *TaskManager) *worker {
	return &worker{
		id:        id,
		task:      make(chan Task),
		taskQueue: m.taskQueue,
		context:   m.context,
		onStart:   m.handleStart,
		onError:   m.handleError,
		onSuccess: m.handleSuccess,
		onTimeout: m.handleWorkerTimeout,
		onStopped: m.handleWorkerStop,
	}
}

func (w *worker) Start() {
	w.task = make(chan Task)
	go func() {
		for {
			select {
			case <-w.context.Done():
				log.Infof("worker %d : stopping", w.id)
				close(w.task)
				w.onStopped(w)
				return
			case w.taskQueue <- w.task:
			}
			select {
			case task := <-w.task:
				log.Infof("worker %d : Received work request", w.id)
				w.onStart(w.id, task.Name())
				err := task.Execute(w.context)
				if err != nil {
					log.Errorf("worker %d : Failed with error : %s", w.id, err.Error())
					w.onError(w.id, task, err)
				} else {
					log.Infof("worker %d : Task finished successfully", w.id)
					w.onSuccess(w.id, task)
				}
			case <-w.context.Done():
				log.Infof("worker %d : stopping", w.id)
				close(w.task)
				w.onStopped(w)
				return
			case <-time.After(time.Second * 15):
				log.Infof("worker %d : idle timeout", w.id)
				close(w.task)
				w.onTimeout(w)
				return
			}
		}
	}()
}
