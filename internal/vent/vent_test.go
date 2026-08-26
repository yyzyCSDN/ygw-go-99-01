package vent

import (
	"context"
	"errors"
	"testing"
	"time"

	"coalminegas/internal/event"
)

// failingActuator 在 Start 阶段即返回错误，模拟现场执行器拒绝启动。
type failingActuator struct{ confirm chan struct{} }

func (a *failingActuator) Start(id string) error { return errors.New("breaker rejected") }
func (a *failingActuator) Stop(id string) error  { return nil }
func (a *failingActuator) Confirm(id string) <-chan struct{} {
	return a.confirm
}

// silentActuator 接受启动指令但永不给出运行确认，模拟现场风机没转起来。
type silentActuator struct{ confirm chan struct{} }

func (a *silentActuator) Start(id string) error { return nil }
func (a *silentActuator) Stop(id string) error  { return nil }
func (a *silentActuator) Confirm(id string) <-chan struct{} {
	return a.confirm
}

func TestStart_RejectedByActuator_Fails(t *testing.T) {
	bus := event.NewBus()
	fan := NewFan("f1", bus, &failingActuator{confirm: make(chan struct{})})

	err := fan.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when actuator rejects start")
	}
	if st := fan.State(); st.State != StateFailed {
		t.Fatalf("state=%s, want %s (现场未启动不应显示已运行)", st.State, StateFailed)
	}
	if st := fan.State(); st.Starts != 0 {
		t.Fatalf("starts=%d, want 0 (未确认成功不应计入启动次数)", st.Starts)
	}
}

func TestStart_NoConfirmBeforeTimeout_Fails(t *testing.T) {
	bus := event.NewBus()
	fan := NewFan("f1", bus, &silentActuator{confirm: make(chan struct{})})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := fan.Start(ctx)
	if err == nil {
		t.Fatal("expected timeout error when no confirm arrives")
	}
	if st := fan.State(); st.State != StateFailed {
		t.Fatalf("state=%s, want %s (超时未收到运行反馈不应置 running)", st.State, StateFailed)
	}
}

func TestStart_Confirmed_Runs(t *testing.T) {
	bus := event.NewBus()
	confirm := make(chan struct{})
	fan := NewFan("f1", bus, &silentActuator{confirm: confirm})

	go func() {
		time.Sleep(5 * time.Millisecond)
		close(confirm)
	}()

	if err := fan.Start(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st := fan.State(); st.State != StateRunning {
		t.Fatalf("state=%s, want %s", st.State, StateRunning)
	}
}

func TestStart_FailedFanCanBeRetried(t *testing.T) {
	bus := event.NewBus()
	confirm := make(chan struct{})
	// 先用失败执行器使风机进入 failed
	fan := NewFan("f1", bus, &failingActuator{confirm: confirm})
	_ = fan.Start(context.Background())
	if st := fan.State(); st.State != StateFailed {
		t.Fatalf("precondition state=%s, want %s", st.State, StateFailed)
	}

	// 值班员重新下发：换用一个会正常确认的执行器
	fan.actor = &silentActuator{confirm: confirm}
	go func() {
		time.Sleep(5 * time.Millisecond)
		close(confirm)
	}()
	if err := fan.Start(context.Background()); err != nil {
		t.Fatalf("retry error: %v (failed 后必须允许重新下发)", err)
	}
	if st := fan.State(); st.State != StateRunning {
		t.Fatalf("state=%s, want %s", st.State, StateRunning)
	}
}
