package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/bronze"
	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/nhl"
	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/season"
)

const (
	dateLayout = "2006-01-02"
	opTimeout  = 30 * time.Second
)

func main() {
	startFlag := flag.String("start", "", "start date (YYYY-MM-DD, inclusive)")
	endFlag := flag.String("end", "", "end date (YYYY-MM-DD, inclusive)")
	endpointFlag := flag.String("s3-endpoint", "", "S3-compatible endpoint URL (credentials read from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars via the AWS SDK default chain)")
	bucketFlag := flag.String("s3-bucket", "", "S3 bucket to write bronze data to")
	seasonFlag := flag.String("season", "", "Season in the format YYYYYYYY such as 20242025")
	flag.Parse()

	var start, end time.Time
	switch {
	case *seasonFlag != "" && (*startFlag != "" || *endFlag != ""):
		log.Fatalf("--season and --start/--end can't be used together")
	case *seasonFlag != "":
		var err error
		start, end, err = season.Dates(*seasonFlag)
		if err != nil {
			log.Fatalf("parse --season: %v", err)
		}
	case *startFlag != "" && *endFlag != "":
		var err error
		start, err = time.Parse(dateLayout, *startFlag)
		if err != nil {
			log.Fatalf("parse --start: %v", err)
		}
		end, err = time.Parse(dateLayout, *endFlag)
		if err != nil {
			log.Fatalf("parse --end: %v", err)
		}
	default:
		log.Fatalf("must specify --season or both --start and --end")
	}

	if end.Before(start) {
		log.Fatalf("end (%s) is before start (%s)", end.Format(dateLayout), start.Format(dateLayout))
	}
	if *endpointFlag == "" || *bucketFlag == "" {
		log.Fatalf("--s3-endpoint and --s3-bucket are required")
	}

	ctx := context.Background()

	writer, err := bronze.NewWriter(ctx, bronze.Config{
		Endpoint: *endpointFlag,
		Bucket:   *bucketFlag,
	})
	if err != nil {
		log.Fatalf("init bronze writer: %v", err)
	}

	client := nhl.NewClient()

	var schedulesOK, scheduleFailures int
	var gamesOK, gameFailures int
	var totalBytes int

	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		date := d.Format(dateLayout)

		scheduleCtx, cancel := context.WithTimeout(ctx, opTimeout)
		scheduleBody, err := client.Schedule(scheduleCtx, date)
		cancel()
		if err != nil {
			log.Printf("date=%s schedule fetch error=%v", date, err)
			scheduleFailures++
			continue
		}

		writeCtx, cancel := context.WithTimeout(ctx, opTimeout)
		err = writer.WriteSchedule(writeCtx, date, scheduleBody)
		cancel()
		if err != nil {
			log.Printf("date=%s schedule write error=%v", date, err)
			scheduleFailures++
			continue
		}
		totalBytes += len(scheduleBody)

		games, err := nhl.ParseGames(scheduleBody)
		if err != nil {
			log.Printf("date=%s schedule parse error=%v", date, err)
			scheduleFailures++
			continue
		}
		schedulesOK++

		var datePBPBytes, datePBPFailures int
		for _, g := range games {
			pbpCtx, cancel := context.WithTimeout(ctx, opTimeout)
			pbpBody, err := client.PlayByPlay(pbpCtx, g.ID)
			cancel()
			if err != nil {
				log.Printf("date=%s game=%d pbp fetch error=%v", date, g.ID, err)
				datePBPFailures++
				gameFailures++
				continue
			}

			writeCtx, cancel := context.WithTimeout(ctx, opTimeout)
			err = writer.WritePlayByPlay(writeCtx, g.Season, date, g.ID, pbpBody)
			cancel()
			if err != nil {
				log.Printf("date=%s game=%d pbp write error=%v", date, g.ID, err)
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
