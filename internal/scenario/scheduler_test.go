package scenario

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/notification-system-moxicom/orchestrator/internal/message"
)

// mockSender records all SendDeliveryTask calls for test assertions.
type mockSender struct {
	calls []sendCall
}

type sendCall struct {
	NotificationID string
	SystemID       string
	UserID         string
	Channel        string
}

func (m *mockSender) SendDeliveryTask(_ context.Context, notificationID, systemID, userID, channel string) error {
	m.calls = append(m.calls, sendCall{
		NotificationID: notificationID,
		SystemID:       systemID,
		UserID:         userID,
		Channel:        channel,
	})
	return nil
}

func setupTest(t *testing.T) (*miniredis.Miniredis, *redis.Client, *mockSender) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sender := &mockSender{}

	return mr, rdb, sender
}

func TestStartScenario_FirstStepSentImmediately(t *testing.T) {
	mr, rdb, sender := setupTest(t)
	defer mr.Close()

	s := NewScheduler(rdb, 100*time.Millisecond, sender)

	steps := []message.ScenarioStep{
		{Channel: "email", DelayMs: 5000},
		{Channel: "telegram", DelayMs: 10000},
	}

	s.StartScenario("notif-1", "sys-1", "user-1", steps)

	// First step must be sent immediately
	if len(sender.calls) != 1 {
		t.Fatalf("expected 1 send call, got %d", len(sender.calls))
	}
	if sender.calls[0].Channel != "email" {
		t.Errorf("expected channel 'email', got %q", sender.calls[0].Channel)
	}

	// Hash should exist in Redis
	key := scenarioKey("notif-1", "user-1")
	exists := mr.Exists(key)
	if !exists {
		t.Error("scenario hash should exist in Redis")
	}

	// Delayed queue should have an entry
	members, err := mr.ZMembers(delayedQueueKey)
	if err != nil {
		t.Fatalf("failed to get delayed queue members: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 delayed queue entry, got %d", len(members))
	}
	if members[0] != memberKey("notif-1", "user-1") {
		t.Errorf("unexpected delayed queue member: %s", members[0])
	}
}

func TestMarkSuccess_StopsScenario(t *testing.T) {
	mr, rdb, sender := setupTest(t)
	defer mr.Close()

	s := NewScheduler(rdb, 100*time.Millisecond, sender)

	steps := []message.ScenarioStep{
		{Channel: "email", DelayMs: 5000},
		{Channel: "telegram", DelayMs: 10000},
	}

	s.StartScenario("notif-2", "sys-1", "user-2", steps)

	remaining := s.MarkSuccess("notif-2", "user-2", "email")

	// Should return remaining steps
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining step, got %d", len(remaining))
	}
	if remaining[0].Channel != "telegram" {
		t.Errorf("expected remaining channel 'telegram', got %q", remaining[0].Channel)
	}

	// Hash should be deleted
	key := scenarioKey("notif-2", "user-2")
	if mr.Exists(key) {
		t.Error("scenario hash should be deleted after success")
	}

	// Delayed queue should be empty (key may not exist at all, which is fine)
	members, err := mr.ZMembers(delayedQueueKey)
	if err == nil && len(members) != 0 {
		t.Errorf("delayed queue should be empty, got %d entries", len(members))
	}
}

func TestMarkFailure_AdvancesToNextStep(t *testing.T) {
	mr, rdb, sender := setupTest(t)
	defer mr.Close()

	s := NewScheduler(rdb, 100*time.Millisecond, sender)

	steps := []message.ScenarioStep{
		{Channel: "email", DelayMs: 5000},
		{Channel: "telegram", DelayMs: 10000},
	}

	s.StartScenario("notif-3", "sys-1", "user-3", steps)
	initialCalls := len(sender.calls)

	s.MarkFailure("notif-3", "user-3", "email")

	// The next step should have been sent
	if len(sender.calls) != initialCalls+1 {
		t.Fatalf("expected %d send calls after failure, got %d", initialCalls+1, len(sender.calls))
	}
	lastCall := sender.calls[len(sender.calls)-1]
	if lastCall.Channel != "telegram" {
		t.Errorf("expected fallback channel 'telegram', got %q", lastCall.Channel)
	}

	// active_index should be updated in Redis
	key := scenarioKey("notif-3", "user-3")
	val, err := rdb.HGet(context.Background(), key, "active_index").Result()
	if err != nil {
		t.Fatalf("failed to read active_index: %v", err)
	}
	if val != "1" {
		t.Errorf("expected active_index=1, got %q", val)
	}
}

func TestPoller_ExecutesDelayedSteps(t *testing.T) {
	mr, rdb, sender := setupTest(t)
	defer mr.Close()

	s := NewScheduler(rdb, 50*time.Millisecond, sender)

	steps := []message.ScenarioStep{
		{Channel: "email", DelayMs: 1},    // 1ms delay — practically immediate
		{Channel: "telegram", DelayMs: 1}, // 1ms delay
	}

	s.StartScenario("notif-4", "sys-1", "user-4", steps)

	// Start poller
	ctx, cancel := context.WithCancel(context.Background())
	s.StartPoller(ctx)

	// Wait for the poller to process the delayed item
	time.Sleep(300 * time.Millisecond)

	cancel()
	s.Stop()

	// The first step is sent by StartScenario, the second by the poller
	foundTelegram := false
	for _, call := range sender.calls {
		if call.Channel == "telegram" && call.NotificationID == "notif-4" {
			foundTelegram = true
			break
		}
	}
	if !foundTelegram {
		t.Errorf("poller should have sent the 'telegram' step; calls: %+v", sender.calls)
	}
}

func TestMarkSuccess_WrongChannel_NoEffect(t *testing.T) {
	mr, rdb, sender := setupTest(t)
	defer mr.Close()

	s := NewScheduler(rdb, 100*time.Millisecond, sender)

	steps := []message.ScenarioStep{
		{Channel: "email", DelayMs: 5000},
		{Channel: "telegram", DelayMs: 10000},
	}

	s.StartScenario("notif-5", "sys-1", "user-5", steps)

	// MarkSuccess with wrong channel should have no effect
	remaining := s.MarkSuccess("notif-5", "user-5", "sms")
	if remaining != nil {
		t.Errorf("expected nil remaining for wrong channel, got %+v", remaining)
	}

	// Scenario should still exist
	key := scenarioKey("notif-5", "user-5")
	if !mr.Exists(key) {
		t.Error("scenario should still exist after wrong channel success")
	}
}
