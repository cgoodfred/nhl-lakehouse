package bronze

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	Endpoint string
	Bucket   string
	Region   string
}

type Writer struct {
	client *s3.Client
	bucket string
}

func NewWriter(ctx context.Context, cfg Config) (*Writer, error) {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = true
		// SeaweedFS does not support the streaming/trailer checksums the SDK
		// now sends by default; computing only when explicitly required keeps
		// the SigV4 signature compatible with the server.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	return NewWriterFromClient(client, cfg.Bucket), nil
}

func NewWriterFromClient(client *s3.Client, bucket string) *Writer {
	return &Writer{
		client: client,
		bucket: bucket,
	}
}

func (w *Writer) WriteSchedule(ctx context.Context, date string, body []byte) error {
	return w.put(ctx, ScheduleKey(date), body)
}

func (w *Writer) WritePlayByPlay(ctx context.Context, season int64, date string, gameID int64, body []byte) error {
	return w.put(ctx, PlayByPlayKey(season, date, gameID), body)
}

func (w *Writer) WriteShiftCharts(ctx context.Context, season int64, date string, gameID int64, body []byte) error {
	return w.put(ctx, ShiftChartsKey(season, date, gameID), body)
}

func ScheduleKey(date string) string {
	return fmt.Sprintf("schedule/date=%s/schedule.json", date)
}

func PlayByPlayKey(season int64, date string, gameID int64) string {
	return fmt.Sprintf("play-by-play/season=%d/date=%s/game_%d.json", season, date, gameID)
}

func ShiftChartsKey(season int64, date string, gameID int64) string {
	return fmt.Sprintf("shift-charts/season=%d/date=%s/game_%d.json", season, date, gameID)
}

func (w *Writer) put(ctx context.Context, key string, body []byte) error {
	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func (w *Writer) WriteRunFailures(ctx context.Context, runID string, body []byte) error {
	key := fmt.Sprintf("_runs/run=%s/failures.json", runID)
	return w.put(ctx, key, body)
}

func (w *Writer) WriteRunImpact(ctx context.Context, runID string, body []byte) error {
	key := fmt.Sprintf("_runs/run=%s/impact.json", runID)
	return w.put(ctx, key, body)
}
