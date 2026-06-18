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
	flag.Parse()

	if *startFlag == "" || *endFlag == "" {
		log.Fatalf("--start and --end are required (YYYY-MM-DD, inclusive)")
	}
	if *endpointFlag == "" || *bucketFlag == "" {
		log.Fatalf("--s3-endpoint and --s3-bucket are required")
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
