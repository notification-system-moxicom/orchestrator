package scenario

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/notification-system-moxicom/orchestrator/internal/message"
)

const (
	delayedQueueKey = "scenario:delayed_queue"
	scenarioPrefix  = "scenario:"
	defaultPollBatch = 10
	// TTL buffer added on top of the sum of all step delays.
	ttlBuffer = 10 * time.Minute
)

// DeliveryTaskSender is called by the Scheduler to dispatch a delivery
// task for a specific channel. It replaces the in-memory closure that
// was previously stored in scenarioState.
type DeliveryTaskSender interface {
	SendDeliveryTask(ctx context.Context, notificationID, systemID, userID, channel string) error
}

// Scheduler manages cascading notification scenarios using Redis
// as the backing store, enabling persistence across restarts and
// safe operation with multiple orchestrator replicas.
type Scheduler struct {
	rdb          *redis.Client
	pollInterval time.Duration
	sender       DeliveryTaskSender
	cancel       context.CancelFunc
	done         chan struct{}
}

// scenarioData is the JSON-serializable representation stored in Redis Hash.
type scenarioData struct {
	NotificationID string                 `json:"notification_id"`
	SystemID       string                 `json:"system_id"`
	UserID         string                 `json:"user_id"`
	Steps          []message.ScenarioStep `json:"steps"`
	ActiveIndex    int                    `json:"active_index"`
}

// NewScheduler creates a new Redis-backed Scheduler.
func NewScheduler(rdb *redis.Client, pollInterval time.Duration, sender DeliveryTaskSender) *Scheduler {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	return &Scheduler{
		rdb:          rdb,
		pollInterval: pollInterval,
		sender:       sender,
		done:         make(chan struct{}),
	}
}

// claimScript atomically pops members whose score <= now from the sorted set.
// This ensures that only one replica processes a given delayed step.
var claimScript = redis.NewScript(`
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #members > 0 then
    redis.call('ZREM', KEYS[1], unpack(members))
end
return members
`)

func scenarioKey(notificationID, userID string) string {
	return scenarioPrefix + notificationID + ":" + userID
}

func memberKey(notificationID, userID string) string {
	return notificationID + ":" + userID
}

// StartScenario persists the scenario state in Redis and immediately
// dispatches the first step. If a second step exists, it is scheduled
// in the delayed queue with the appropriate delay.
func (s *Scheduler) StartScenario(
	notificationID string,
	systemID string,
	userID string,
	steps []message.ScenarioStep,
) {
	if len(steps) == 0 {
		return
	}

	ctx := context.Background()
	key := scenarioKey(notificationID, userID)

	data := scenarioData{
		NotificationID: notificationID,
		SystemID:       systemID,
		UserID:         userID,
		Steps:          steps,
		ActiveIndex:    0,
	}

	stepsJSON, err := json.Marshal(data.Steps)
	if err != nil {
		slog.Error("failed to marshal scenario steps", "error", err)
		return
	}

	// Calculate TTL as sum of all delays + buffer
	var totalDelay time.Duration
	for _, step := range steps {
		totalDelay += time.Duration(step.DelayMs) * time.Millisecond
	}
	ttl := totalDelay + ttlBuffer

	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key,
		"notification_id", data.NotificationID,
		"system_id", data.SystemID,
		"user_id", data.UserID,
		"steps", string(stepsJSON),
		"active_index", strconv.Itoa(data.ActiveIndex),
	)
	pipe.Expire(ctx, key, ttl)

	if _, err := pipe.Exec(ctx); err != nil {
		slog.Error("failed to persist scenario state", "error", err, "key", key)
		return
	}

	// Send first step immediately
	if err := s.sender.SendDeliveryTask(ctx, notificationID, systemID, userID, steps[0].Channel); err != nil {
		slog.Error("failed to send first scenario step", "error", err,
			"notification_id", notificationID, "user_id", userID, "channel", steps[0].Channel)
	}

	// Schedule the next check — after the delay of the first step
	if len(steps) > 1 {
		executeAt := time.Now().Add(time.Duration(steps[0].DelayMs) * time.Millisecond)
		member := memberKey(notificationID, userID)
		if err := s.rdb.ZAdd(ctx, delayedQueueKey, redis.Z{
			Score:  float64(executeAt.UnixMilli()),
			Member: member,
		}).Err(); err != nil {
			slog.Error("failed to schedule next scenario step", "error", err, "key", key)
		}
	}
}

// MarkSuccess is called when a delivery for a channel succeeds.
// It removes the scenario from Redis and returns the remaining steps
// (which should be emitted as "skipped").
func (s *Scheduler) MarkSuccess(notificationID, userID, channel string) []message.ScenarioStep {
	ctx := context.Background()
	key := scenarioKey(notificationID, userID)

	data, err := s.loadScenario(ctx, key)
	if err != nil || data == nil {
		return nil
	}

	if data.ActiveIndex >= len(data.Steps) || data.Steps[data.ActiveIndex].Channel != channel {
		return nil
	}

	remaining := make([]message.ScenarioStep, len(data.Steps[data.ActiveIndex+1:]))
	copy(remaining, data.Steps[data.ActiveIndex+1:])

	// Clean up: remove hash and sorted set entry
	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, key)
	pipe.ZRem(ctx, delayedQueueKey, memberKey(notificationID, userID))
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Error("failed to clean up scenario after success", "error", err, "key", key)
	}

	return remaining
}

// MarkFailure is called when a delivery for a channel fails.
// It advances to the next step, immediately dispatches it, and
// schedules the step after that in the delayed queue.
func (s *Scheduler) MarkFailure(notificationID, userID, channel string) {
	ctx := context.Background()
	key := scenarioKey(notificationID, userID)

	data, err := s.loadScenario(ctx, key)
	if err != nil || data == nil {
		return
	}

	if data.ActiveIndex >= len(data.Steps) || data.Steps[data.ActiveIndex].Channel != channel {
		return
	}

	// Remove pending delayed trigger for the current step
	s.rdb.ZRem(ctx, delayedQueueKey, memberKey(notificationID, userID))

	nextIndex := data.ActiveIndex + 1
	if nextIndex >= len(data.Steps) {
		// No more steps — clean up
		s.rdb.Del(ctx, key)
		return
	}

	// Update active_index in Redis
	if err := s.rdb.HSet(ctx, key, "active_index", strconv.Itoa(nextIndex)).Err(); err != nil {
		slog.Error("failed to advance scenario step", "error", err, "key", key)
		return
	}

	// Send the next step immediately
	nextStep := data.Steps[nextIndex]
	if err := s.sender.SendDeliveryTask(ctx, notificationID, data.SystemID, userID, nextStep.Channel); err != nil {
		slog.Error("failed to send scenario fallback step", "error", err,
			"notification_id", notificationID, "user_id", userID, "channel", nextStep.Channel)
	}

	// Schedule the step after the next one
	if nextIndex+1 < len(data.Steps) {
		executeAt := time.Now().Add(time.Duration(nextStep.DelayMs) * time.Millisecond)
		if err := s.rdb.ZAdd(ctx, delayedQueueKey, redis.Z{
			Score:  float64(executeAt.UnixMilli()),
			Member: memberKey(notificationID, userID),
		}).Err(); err != nil {
			slog.Error("failed to schedule next scenario step after failure", "error", err, "key", key)
		}
	} else {
		// Last step — no more delays, clean up after it completes
	}
}

// StartPoller launches a background goroutine that periodically checks
// the delayed queue for steps that are ready to be executed.
func (s *Scheduler) StartPoller(ctx context.Context) {
	pollCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	go func() {
		defer close(s.done)
		ticker := time.NewTicker(s.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-pollCtx.Done():
				return
			case <-ticker.C:
				s.poll(pollCtx)
			}
		}
	}()
}

// Stop gracefully shuts down the poller.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	<-s.done
}

func (s *Scheduler) poll(ctx context.Context) {
	nowMs := time.Now().UnixMilli()

	result, err := claimScript.Run(ctx, s.rdb,
		[]string{delayedQueueKey},
		nowMs,
		defaultPollBatch,
	).StringSlice()

	if err != nil && err != redis.Nil {
		slog.Error("scenario poll: failed to claim delayed items", "error", err)
		return
	}

	for _, member := range result {
		s.processDelayedMember(ctx, member)
	}
}

func (s *Scheduler) processDelayedMember(ctx context.Context, member string) {
	// member format: "{notificationId}:{userId}"
	key := scenarioPrefix + member

	data, err := s.loadScenario(ctx, key)
	if err != nil || data == nil {
		return
	}

	nextIndex := data.ActiveIndex + 1
	if nextIndex >= len(data.Steps) {
		// No more steps to execute — clean up
		s.rdb.Del(ctx, key)
		return
	}

	// Advance active_index
	if err := s.rdb.HSet(ctx, key, "active_index", strconv.Itoa(nextIndex)).Err(); err != nil {
		slog.Error("scenario poll: failed to advance step", "error", err, "key", key)
		return
	}

	nextStep := data.Steps[nextIndex]
	if err := s.sender.SendDeliveryTask(ctx, data.NotificationID, data.SystemID, data.UserID, nextStep.Channel); err != nil {
		slog.Error("scenario poll: failed to send step", "error", err,
			"notification_id", data.NotificationID, "user_id", data.UserID, "channel", nextStep.Channel)
	}

	// Schedule the subsequent step if one exists
	if nextIndex+1 < len(data.Steps) {
		executeAt := time.Now().Add(time.Duration(nextStep.DelayMs) * time.Millisecond)
		if err := s.rdb.ZAdd(ctx, delayedQueueKey, redis.Z{
			Score:  float64(executeAt.UnixMilli()),
			Member: member,
		}).Err(); err != nil {
			slog.Error("scenario poll: failed to schedule next step", "error", err, "key", key)
		}
	}
}

func (s *Scheduler) loadScenario(ctx context.Context, key string) (*scenarioData, error) {
	vals, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load scenario %s: %w", key, err)
	}
	if len(vals) == 0 {
		return nil, nil
	}

	var steps []message.ScenarioStep
	if err := json.Unmarshal([]byte(vals["steps"]), &steps); err != nil {
		return nil, fmt.Errorf("failed to unmarshal steps for %s: %w", key, err)
	}

	activeIndex, _ := strconv.Atoi(vals["active_index"])

	return &scenarioData{
		NotificationID: vals["notification_id"],
		SystemID:       vals["system_id"],
		UserID:         vals["user_id"],
		Steps:          steps,
		ActiveIndex:    activeIndex,
	}, nil
}
