package scenario

import (
	"log/slog"
	"sync"
	"time"

	"github.com/notification-system-moxicom/orchestrator/internal/message"
)

type Scheduler struct {
	mu        sync.Mutex
	scenarios map[string]*scenarioState
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		scenarios: make(map[string]*scenarioState),
	}
}

type scenarioState struct {
	steps       []message.ScenarioStep
	activeIndex int
	timer       *time.Timer
	send        func(step message.ScenarioStep) error
}

func (s *Scheduler) StartScenario(
	notificationID string,
	systemID string,
	userID string,
	steps []message.ScenarioStep,
	send func(step message.ScenarioStep) error,
) {
	// TODO: This scheduler is in-memory and single-instance only.
	// When running multiple orchestrator replicas, scenario state must be moved
	// to a shared store (e.g., Redis delayed queue or Postgres state + workers)
	// to avoid cross-replica race conditions and duplicate fallback sends.
	if len(steps) == 0 {
		return
	}

	key := s.key(notificationID, userID)

	s.mu.Lock()
	if existing := s.scenarios[key]; existing != nil && existing.timer != nil {
		existing.timer.Stop()
	}
	s.scenarios[key] = &scenarioState{
		steps:       steps,
		activeIndex: 0,
		send:        send,
	}
	s.mu.Unlock()

	if err := send(steps[0]); err != nil {
		slog.Error("failed to send scenario step", "notification_id", notificationID, "user_id", userID, "channel", steps[0].Channel, "error", err)
	}

	s.scheduleNext(notificationID, userID, 0)
}

func (s *Scheduler) MarkSuccess(notificationID string, userID string, channel string) []message.ScenarioStep {
	key := s.key(notificationID, userID)

	s.mu.Lock()
	state, ok := s.scenarios[key]
	if !ok || state == nil {
		s.mu.Unlock()
		return nil
	}

	if state.activeIndex >= len(state.steps) || state.steps[state.activeIndex].Channel != channel {
		s.mu.Unlock()
		return nil
	}

	if state.timer != nil {
		state.timer.Stop()
	}

	remaining := append([]message.ScenarioStep(nil), state.steps[state.activeIndex+1:]...)
	delete(s.scenarios, key)
	s.mu.Unlock()

	return remaining
}

func (s *Scheduler) MarkFailure(notificationID string, userID string, channel string) {
	key := s.key(notificationID, userID)

	s.mu.Lock()
	state, ok := s.scenarios[key]
	if !ok || state == nil {
		s.mu.Unlock()
		return
	}

	if state.activeIndex >= len(state.steps) || state.steps[state.activeIndex].Channel != channel {
		s.mu.Unlock()
		return
	}

	if state.timer != nil {
		state.timer.Stop()
	}

	nextIndex := state.activeIndex + 1
	if nextIndex >= len(state.steps) {
		delete(s.scenarios, key)
		s.mu.Unlock()
		return
	}

	state.activeIndex = nextIndex
	send := state.send
	nextStep := state.steps[nextIndex]
	s.mu.Unlock()

	if err := send(nextStep); err != nil {
		slog.Error("failed to send scenario fallback step", "notification_id", notificationID, "user_id", userID, "channel", nextStep.Channel, "error", err)
	}

	s.scheduleNext(notificationID, userID, nextIndex)
}

func (s *Scheduler) scheduleNext(notificationID string, userID string, currentIndex int) {
	key := s.key(notificationID, userID)

	s.mu.Lock()
	state, ok := s.scenarios[key]
	if !ok || state == nil {
		s.mu.Unlock()
		return
	}

	if currentIndex >= len(state.steps)-1 {
		delete(s.scenarios, key)
		s.mu.Unlock()
		return
	}

	if state.activeIndex != currentIndex {
		s.mu.Unlock()
		return
	}

	delay := time.Duration(state.steps[currentIndex].DelayMs) * time.Millisecond
	timer := time.AfterFunc(delay, func() {
		s.mu.Lock()
		state, ok := s.scenarios[key]
		if !ok || state == nil {
			s.mu.Unlock()
			return
		}
		if state.activeIndex != currentIndex {
			s.mu.Unlock()
			return
		}
		nextIndex := currentIndex + 1
		if nextIndex >= len(state.steps) {
			delete(s.scenarios, key)
			s.mu.Unlock()
			return
		}
		state.activeIndex = nextIndex
		send := state.send
		nextStep := state.steps[nextIndex]
		s.mu.Unlock()

		if err := send(nextStep); err != nil {
			slog.Error("failed to send scenario fallback step", "notification_id", notificationID, "user_id", userID, "channel", nextStep.Channel, "error", err)
		}

		s.scheduleNext(notificationID, userID, nextIndex)
	})

	if state.timer != nil {
		state.timer.Stop()
	}
	state.timer = timer
	s.mu.Unlock()
}

func (s *Scheduler) key(notificationID string, userID string) string {
	return notificationID + ":" + userID
}
