package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/nhl"
)

const dateLayout = "2006-01-02"

func main() {
	startFlag := flag.String("start", "", "start date (YYYY-MM-DD, inclusive)")
	endFlag := flag.String("end", "", "end date (YYYY-MM-DD, inclusive)")
	flag.Parse()

	if *startFlag == "" || *endFlag == "" {
		log.Fatalf("--start and --end are required (YYYY-MM-DD, inclusive)")
	}

	start, err := time.Parse(dateLayout, *startFlag)
	if err != nil {
		log.Fatalf("parse --start: %v", err)
	}
	end, err := time.Parse(dateLayout, *endFlag)
	if err != nil {
		log.Fatalf("parse --end: %v", err)
	}
	if end.Before(start) {
		log.Fatalf("--end (%s) is before --start (%s)", *endFlag, *startFlag)
	}

	client := nhl.NewClient()

	var schedulesOK, scheduleFailures int
	var gamesOK, gameFailures int
	var totalBytes int

	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		date := d.Format(dateLayout)

		scheduleCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		scheduleBody, err := client.Schedule(scheduleCtx, date)
		cancel()
		if err != nil {
			log.Printf("date=%s schedule error=%v", date, err)
			scheduleFailures++
			continue
		}
		totalBytes += len(scheduleBody)

		games, err := nhl.ParseGames(scheduleBody)
		if err != nil {
			log.Printf("date=%s parse error=%v", date, err)
			scheduleFailures++
			continue
		}
		schedulesOK++

		var datePBPBytes, datePBPFailures int
		for _, g := range games {
			pbpCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			pbpBody, err := client.PlayByPlay(pbpCtx, g.ID)
			cancel()
			if err != nil {
				log.Printf("date=%s game=%d pbp error=%v", date, g.ID, err)
				datePBPFailures++
				gameFailures++
				continue
			}
			gamesOK++
			datePBPBytes += len(pbpBody)
			totalBytes += len(pbpBody)
		}

		fmt.Printf("date=%s schedule_bytes=%d games=%d pbp_bytes=%d pbp_failures=%d\n",
			date, len(scheduleBody), len(games), datePBPBytes, datePBPFailures)
	}

	fmt.Printf("done schedules_ok=%d schedule_failures=%d games_ok=%d game_failures=%d total_bytes=%d\n",
		schedulesOK, scheduleFailures, gamesOK, gameFailures, totalBytes)

	if scheduleFailures > 0 || gameFailures > 0 {
		os.Exit(1)
	}
}
