package taskmanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var logg = logging.Logger("manager_test")

type taskOne struct {
	name string
}

type taskTwo struct {
	name string
}

func (tOne *taskOne) Execute(ctx context.Context) error {
	<-time.After(time.Second * 3)
	return nil
}

func (tOne *taskOne) Name() string {
	return tOne.name
}

func (tTwo *taskTwo) Execute(ctx context.Context) error {
	<-time.After(time.Second * 20)
	return nil
}

func (tTwo *taskTwo) Name() string {
	return tTwo.name
}

type taskThree struct {
	name        string
	progress    float64
	errDuration time.Duration
}

func (tThree *taskThree) Execute(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	if tThree.errDuration == 0 {
		tThree.errDuration = time.Hour
	}
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			tThree.progress += 25
			if tThree.progress == 100 {
				return nil
			}
		case <-time.After(tThree.errDuration):
			return errors.New("dummy error")
		case <-ctx.Done():
			return nil
		}
	}
}

func (tThree *taskThree) Name() string {
	return tThree.name
}

func (tThree *taskThree) Progress() (float64, error) {
	return tThree.progress, nil
}

func (tThree *taskThree) Description() string {
	return "Test task with dummy progress"
}

func (tThree *taskThree) Pause() {}

type taskFour struct {
	*taskThree
	restarted bool
}

func (tFour *taskFour) Restart() {
	tFour.restarted = true
	tFour.taskThree.errDuration = 0
}

type taskFive struct {
	name string
}

func (t *taskFive) Execute(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Debug("running task five")
		case <-ctx.Done():
			return nil
		}
	}
}

func (t *taskFive) Name() string {
	return t.name
}

const workerCount int32 = 3

var tm *TaskManager

func TestMain(m *testing.M) {
	logging.SetLogLevel("manager_test", "Debug")
	logging.SetLogLevel("taskmanager", "Debug")
	tm = NewTaskManager(context.Background(), workerCount)

	code := m.Run()
	os.Exit(code)
}

func verifyWorkerCount(t *testing.T, count int) {
	if len(tm.Status()) != count {
		t.Fatalf("Invalid worker count Exp: %d Found: %d", count, len(tm.Status()))
	}
	if tm.running != int32(count) {
		t.Fatalf("Invalid running count Exp: %d Found:%d", count, tm.running)
	}
}

func verifyWorkerInfo(t *testing.T, wId int32, tName, state string) {
	st, ok := tm.Status()[wId]
	if !ok {
		t.Fatal("Worker ID not present")
	}
	if len(tName) > 0 && st.TaskName != tName {
		t.Fatal("Invalid task name in worker info")
	}
	if st.Status != state {
		t.Fatal("Invalid status in worker info")
	}
}

func verifyProgressCount(t *testing.T, count int) {
	if len(tm.TaskWithProgressStatus()) != count {
		t.Fatalf("ProgressTask count incorrect Exp: %d Found: %d",
			count, len(tm.TaskWithProgressStatus()))
	}
}

func TestInitialState(t *testing.T) {
	log.Debug("Running TestInitialState")
	verifyWorkerCount(t, 1)
	verifyWorkerInfo(t, 1, "Idle", "waiting")
}

// Test to see additional workers being added correctly
// on addition of more tasks than initial workers
func TestStartWorkerChange(t *testing.T) {
	log.Debug("Running TestStartWorkerChange")
	task := &taskOne{
		name: "Alison",
	}
	log.Debug("Starting 1st task")
	tm.GoWork(task)
	<-time.After(time.Second * 1)
	// Worker count should be unaffected, first worker should be assigned
	log.Debug("Verifying 1st task")
	verifyWorkerCount(t, 1)
	verifyWorkerInfo(t, 1, task.Name(), "running")
	task2 := &taskTwo{
		name: "Bob",
	}
	log.Debug("Starting 2nd task")
	tm.GoWork(task2)
	<-time.After(time.Second * 1)
	// Worker count should increase, one additional worker should be started
	// worker no. 1 should be assigned
	log.Debug("Verifying 2nd task")
	verifyWorkerCount(t, 2)
	verifyWorkerInfo(t, 2, task2.Name(), "running")
	// After 15 seconds, TaskOne will be done. Worker should timeout and stop.
	// We should see worker could reach 1 again. Second worker should still be
	// running
	<-time.After(time.Second * 19)
	log.Debug("Verifying 2nd task still running")
	verifyWorkerCount(t, 1)
	verifyWorkerInfo(t, 2, task2.Name(), "running")
	// Now taskTwo should be done. Worker should be back in idle state
	<-time.After(time.Second * 2)
	log.Debug("Verifying all idle")
	verifyWorkerCount(t, 1)
	verifyWorkerInfo(t, 2, "Idle", "waiting")
}

func TestStartMoreTasks(t *testing.T) {
	log.Debug("Running TestStartMoreTasks")
	for i := 0; i < 4; i++ {
		t := &taskOne{
			name: fmt.Sprintf("Hell %d", i),
		}
		tm.GoWork(t)
		<-time.After(time.Millisecond * 500)
	}
	<-time.After(time.Millisecond * 500)
	log.Debug("Verifying all 3 tasks are running")
	verifyWorkerCount(t, 3)
	// Worker 0 was stopped in previous test. Now we should have three workers
	// 1, 2 and 3. All should be in running state
	for i := 0; i < 3; i++ {
		verifyWorkerInfo(t, int32(2+i), "", "running")
	}
	<-time.After(time.Second * 1)
	// First task should be over by now. Worker 1 should be reassigned with new task
	log.Debug("Verifying new running")
	verifyWorkerCount(t, 3)
	verifyWorkerInfo(t, 2, "", "running")
	<-time.After(time.Second * 3)
	log.Debug("Verifying workers idle")
	verifyWorkerCount(t, 3)
	for i := 0; i < 3; i++ {
		verifyWorkerInfo(t, int32(2+i), "Idle", "waiting")
	}
	<-time.After(time.Second * 15)
	log.Debug("Verifying workers stopped")
	verifyWorkerCount(t, 1)
}

func TestTasksWithProgressStop(t *testing.T) {
	log.Debug("Running TestTasksWithProgress")
	t1 := &taskThree{
		name: "Progress Task",
	}
	tm.GoWork(t1)
	<-time.After(time.Millisecond * 100)
	verifyWorkerCount(t, 1)
	verifyProgressCount(t, 1)
	// Progress task with same name should not be enqueued. No new worker will
	// be started
	t2 := &taskThree{
		name: "Progress Task",
	}
	tm.GoWork(t2)
	<-time.After(time.Millisecond * 100)
	log.Debug("Verifying same progress task not enqueued")
	verifyWorkerCount(t, 1)
	verifyProgressCount(t, 1)
	// After task name is modified. Task should be enqueued. New worker will also
	// be started
	t2.name += " modified"
	tm.GoWork(t2)
	<-time.After(time.Second * 1)
	log.Debug("Verifying task enqueued after modifying")
	verifyWorkerCount(t, 2)
	verifyProgressCount(t, 2)
	t3 := &taskThree{
		name:        "Progress Task with error",
		errDuration: time.Second * 5,
	}
	tm.GoWork(t3)
	<-time.After(time.Second * 1)
	log.Debug("Verifying task with error enqueued")
	verifyWorkerCount(t, 3)
	verifyProgressCount(t, 3)
	<-time.After(time.Second * 7)
	log.Debug("Verifying task with error is cleaned up")
	// Task with error will have returned error
	verifyProgressCount(t, 2)
	// Restartable task
	t4 := &taskFour{
		taskThree: &taskThree{
			name:        "Restartable Progress Task",
			errDuration: time.Second * 5,
		},
	}
	tm.GoWork(t4)
	<-time.After(time.Millisecond * 100)
	verifyProgressCount(t, 3)
	<-time.After(time.Second * 12)
	// Task with error will have returned error
	log.Debug("Verifying task restarted")
	verifyProgressCount(t, 3)
	if !t4.restarted {
		t.Fatal("Restart was not called on error")
	}
	log.Debug("Verifying progress updated")
	for nm, v := range tm.TaskWithProgressStatus() {
		if nm == t1.Name() || nm == t2.Name() {
			val, err := v.Progress()
			if err != nil {
				t.Fatalf("Error getting progress %s", err.Error())
			}
			if val <= 0 && val >= 100 {
				t.Fatalf("Task progress incorrect %f", val)
			}
		}
	}
	<-time.After(time.Second * 20)
	log.Debug("Verifying progress tasks completed")
	verifyProgressCount(t, 1)
	if t1.progress != 100 || t2.progress != 100 {
		t.Fatal("Tasks not completed")
	}
	stopStart := time.Now()
	tm.Stop()
	if time.Since(stopStart) > time.Second*2 {
		t.Fatal("Took too long to start")
	}
	verifyProgressCount(t, 0)
	verifyWorkerCount(t, 0)
}

func TestWorkerQueueBug(t *testing.T) {
	m := NewTaskManager(context.Background(), 3000)

	// let the tasks get timedout
	<-time.After(time.Second * 16)

	// Start 30 tasks
	for i := 0; i < 30; i++ {
		task := &taskFive{
			name: fmt.Sprintf("Alison %d", i),
		}
		logg.Debug("Starting task ", task.Name())
		m.GoWork(task)
	}
	<-time.After(time.Second * 30)
	m.Stop()
	<-time.After(time.Second * 30)

	// After Stop Status and TaskName should be updated
	for _, v := range m.workerMap {
		if v.Status != "waiting" {
			t.Fatal("status should be waiting but got ", v.Status)
		}
		if v.TaskName != "Idle" {
			t.Fatal("name should be Idle but got ", v.TaskName)
		}
	}
}

type panicTask struct{}

func (p *panicTask) Execute(ctx context.Context) error {
	<-time.After(time.Second * 5)
	panic("PANIC!")
	return nil
}

func (p *panicTask) Name() string {
	return "panicTask"
}

type panicRestartTask struct {
	restarted bool
}

func (p *panicRestartTask) Execute(ctx context.Context) error {
	<-time.After(time.Second * 5)
	if !p.restarted {
		panic("PANIC!")
	} else {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				log.Debug("Waiting to stop")
			}
		}
	}
	return nil
}

func (p *panicRestartTask) Restart(ctx context.Context) {
	log.Debug("Restarting panic task")
	p.restarted = true
}

func (p *panicRestartTask) Name() string {
	return "panicRestartTask"
}

func TestWorkerPanic(t *testing.T) {
	log.Debug("Running TestWorkerPanic")
	log.Debug("Starting panic task")
	tm.GoWork(&panicTask{})
	<-time.After(time.Second * 1)
	// Worker count should be unaffected, first worker should be assigned
	log.Debug("Verifying 1st task")
	verifyWorkerCount(t, 1)
	verifyWorkerInfo(t, 1, "panicTask", "running")
	<-time.After(time.Second * 5)
	verifyWorkerCount(t, 0)
	log.Debug("Starting panic restartable task")
	rt := &panicRestartTask{}
	tm.GoWork(rt)
	<-time.After(time.Second * 1)
	// At this point, first worker is stopped, so manager will start 2 new
	// workers. The task will close another, so we will only verify at the end
	verifyWorkerCount(t, 2)
	<-time.After(time.Second * 10)
	if !rt.restarted {
		t.Fatal("Task was not restarted")
	}
	verifyWorkerCount(t, 1)
	for _, v := range tm.Status() {
		if v.TaskName != "panicRestartTask" && v.Status != "running" {
			t.Fatal("Task status invalid")
		}
	}
}
