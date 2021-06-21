package main

import (
	"log"
	"os"

	"tp2.aba.distros.fi.uba.ar/node/client"
	"tp2.aba.distros.fi.uba.ar/node/collector"
	"tp2.aba.distros.fi.uba.ar/node/filter"
	"tp2.aba.distros.fi.uba.ar/node/join"
	"tp2.aba.distros.fi.uba.ar/node/sink"
)

func main() {
	// Run the specific node depending on program parameters.
	program := os.Args[1]

	log.Printf("starting program: %s\n", program)

	switch program {
	case "client":
		client.Run()
	case "lmfilter":
		filter.RunLongMatchFilter()
	case "lrdfilter":
		filter.RunLargeRatingDifferenceFilter()
	case "top5filter":
		filter.RunCivilizationUsageCountFilter()
	case "civperformancefilter":
		filter.RunCivilizationVictoryDataFilter()
	case "join":
		join.Run()
	case "victorydatacollector":
		collector.RunCivilizationVictoryDataCollector()
	case "usagedatacollector":
		collector.RunCivilizationUsageDataCollector()
	case "sink":
		sink.Run()
	default:
		log.Println("unexpected program name")
	}
}
