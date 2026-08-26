package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"coalminegas/internal/alarm"
	"coalminegas/internal/console"
	"coalminegas/internal/event"
	"coalminegas/internal/gas"
	"coalminegas/internal/record"
	"coalminegas/internal/sensor"
	"coalminegas/internal/trip"
	"coalminegas/internal/vent"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	storeMode := flag.String("record-store", "disk", "record store: disk or memory")
	flag.Parse()

	bus := event.NewBus()
	dispatcher := alarm.NewDispatcher()
	_ = dispatcher.Attach(alarm.Device{ID: "sound-1"})
	_ = dispatcher.Attach(alarm.Device{ID: "light-1"})
	alarms := alarm.NewManager(bus, dispatcher)
	breaker := trip.NewSimulatedBreaker(150 * time.Millisecond)
	trips := trip.NewManager(bus, breaker)
	registry := sensor.NewRegistry(3 * time.Minute)
	_ = registry.Register("p01", "采区一")
	_ = registry.Register("p02", "采区二")
	_ = registry.Register("p03", "采区三")

	var opener record.FileOpener
	if *storeMode == "memory" {
		opener = record.NewMemoryStore()
	} else {
		opener = record.NewDiskStore()
	}
	rotator := record.NewRotation("records", "gas", 2000)
	recorder := record.NewRecorder(opener, bus, rotator)

	interlock := vent.NewInterlock()
	supervisor := gas.NewSupervisor(gas.Config{
		Thresholds:     gas.Thresholds{Alert: 0.8, Trip: 1.2},
		Bus:            bus,
		Trips:          trips,
		Alarms:         alarms,
		Records:        recorder,
		Sensors:        registry,
		Interlock:      interlock,
		HoldTTL:        5 * time.Minute,
		TripTimeout:    8 * time.Second,
		SampleInterval: 1 * time.Second,
	})
	sensorUnit := gas.NewSensor(registry)
	for _, id := range []string{"p01", "p02", "p03"} {
		supervisor.AddFan(id, vent.NewFan(id, bus, vent.NewSimulatedActuator()))
	}
	_ = supervisor.Recover(map[string]gas.PointState{})

	con := console.NewConsole(supervisor, registry, alarms, recorder, trips, bus)
	defer con.Close()
	api := console.NewAPI(con)
	mux := http.NewServeMux()
	mux.Handle("/api/", api)
	mux.Handle("/web/", http.StripPrefix("/web/", http.FileServer(http.Dir("web"))))
	mux.HandleFunc("/", serveIndex)

	poller := newPoller(supervisor, sensorUnit)
	poller.Start()
	defer poller.Stop()

	server := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("server error: %v", err)
		}
	}()
	log.Printf("coal mine gas safety console listening on %s", *addr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	content, err := os.ReadFile("web/console.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(content)
}

type poller struct {
	sup    *gas.Supervisor
	sensor *gas.Sensor
	stop   chan struct{}
	done   chan struct{}
}

func newPoller(sup *gas.Supervisor, sensorUnit *gas.Sensor) *poller {
	return &poller{
		sup:    sup,
		sensor: sensorUnit,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (p *poller) Start() {
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				for _, id := range []string{"p01", "p02", "p03"} {
					reading, err := p.sensor.Poll(id)
					if err != nil {
						continue
					}
					_, _ = p.sup.Ingest(reading)
				}
			}
		}
	}()
}

func (p *poller) Stop() {
	close(p.stop)
	<-p.done
}
