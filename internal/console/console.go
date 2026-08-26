package console

import (
	"sort"
	"sync"
	"time"

	"coalminegas/internal/alarm"
	"coalminegas/internal/event"
	"coalminegas/internal/gas"
	"coalminegas/internal/record"
	"coalminegas/internal/sensor"
	"coalminegas/internal/trip"
)

type Console struct {
	sup    *gas.Supervisor
	reg    *sensor.Registry
	alarms *alarm.Manager
	rec    *record.Recorder
	trips  *trip.Manager
	bus    *event.Bus
	mu     sync.Mutex
	events []string
	subID  int
}

func NewConsole(sup *gas.Supervisor, reg *sensor.Registry, alarms *alarm.Manager, rec *record.Recorder, trips *trip.Manager, bus *event.Bus) *Console {
	con := &Console{
		sup:    sup,
		reg:    reg,
		alarms: alarms,
		rec:    rec,
		trips:  trips,
		bus:    bus,
	}
	con.subID = bus.Register("", func(topic string, payload any) {
		con.mu.Lock()
		defer con.mu.Unlock()
		con.events = append(con.events, topic)
		if len(con.events) > 64 {
			con.events = con.events[len(con.events)-64:]
		}
	})
	return con
}

func (c *Console) Close() {
	c.bus.Deregister(c.subID)
}

func (c *Console) RecentEvents() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.events...)
	return out
}

type Summary struct {
	Points         int
	PointDetails   []gas.PointState
	Alerted        int
	Tripped        int
	Restored       int
	Holds          int
	FansRunning    int
	FansFailed     int
	FanStarts      int
	FanStops       int
	FanHours       float64
	FanFailures    map[string]string
	AutoDisabled   []string
	Alarms         int
	AlarmsRaised   int
	AlarmsCleared  int
	DispatchHits   int
	Records        int
	RecordFails    int
	Online         int
	SensorsTotal   int
	Stale          int
	Locks          int
	ExecutingLocks int
	TrippedLocks   int
	ArmedLocks     int
	LockReasons    map[string]string
	LockUpdatedAt  map[string]string
	Events         int
	Topics         int
}

func (c *Console) Summary() Summary {
	counts := c.sup.Counts()
	fans := c.sup.Fans()
	appends, fails := c.rec.Stats()
	health := c.reg.Health(time.Now(), 3*time.Minute)
	locks := c.trips.Locks()
	hours := 0.0
	starts := 0
	stops := 0
	failures := make(map[string]string)
	autoDisabled := make([]string, 0)
	for _, fan := range fans {
		hours += fan.Hours
		starts += fan.Starts
		stops += fan.Stops
		if fan.State == "failed" && fan.Reason != "" {
			failures[fan.ID] = fan.Reason
		}
		if fan.AutoOff {
			autoDisabled = append(autoDisabled, fan.ID)
		}
	}
	sort.Strings(autoDisabled)
	lockReasons := make(map[string]string)
	lockUpdated := make(map[string]string)
	for _, lock := range locks {
		lockReasons[lock.ID] = lock.Reason
		lockUpdated[lock.ID] = lock.UpdatedAt.Format(time.RFC3339)
	}
	return Summary{
		Points:         c.sup.Table().Len(),
		PointDetails:   c.sup.Table().Points(),
		Alerted:        counts.Alerted,
		Tripped:        counts.Tripped,
		Restored:       counts.Restored,
		Holds:          counts.Holds,
		FansRunning:    runningFans(fans),
		FansFailed:     failedFans(fans),
		FanStarts:      starts,
		FanStops:       stops,
		FanHours:       hours,
		FanFailures:    failures,
		AutoDisabled:   autoDisabled,
		Alarms:         c.alarms.ActiveCount(),
		AlarmsRaised:   c.alarms.Raised(),
		AlarmsCleared:  c.alarms.Cleared(),
		DispatchHits:   c.alarms.DispatchHits(),
		Records:        appends,
		RecordFails:    fails,
		Online:         health.Online,
		SensorsTotal:   health.Total,
		Stale:          health.Stale,
		Locks:          len(locks),
		ExecutingLocks: c.trips.Executing(),
		TrippedLocks:   c.trips.Tripped(),
		ArmedLocks:     c.trips.Armed(),
		LockReasons:    lockReasons,
		LockUpdatedAt:  lockUpdated,
		Events:         c.bus.Count(),
		Topics:         len(c.sup.Bus().Topics()),
	}
}

func runningFans(fans []gas.FanStatus) int {
	count := 0
	for _, fan := range fans {
		if fan.State == "running" {
			count++
		}
	}
	return count
}

func failedFans(fans []gas.FanStatus) int {
	count := 0
	for _, fan := range fans {
		if fan.State == "failed" {
			count++
		}
	}
	return count
}

func (c *Console) CommandStartFan(id string, timeout time.Duration) error {
	return c.sup.StartFan(id, timeout)
}

func (c *Console) CommandTriggerTrip(id string, timeout time.Duration) error {
	return c.sup.TriggerTrip(id, timeout)
}

func (c *Console) CommandResetTrip(id string) error {
	return c.sup.ResetTrip(id)
}

func (c *Console) CommandRestore(id string) error {
	return c.sup.ManualRestore(id)
}

func (c *Console) CommandStopAfterStable(id string, window time.Duration) error {
	return c.sup.RequestFanStop(id, window)
}

func (c *Console) CommandCalibrate(id string, value float64) error {
	return c.sup.Calibrate(id, value)
}
