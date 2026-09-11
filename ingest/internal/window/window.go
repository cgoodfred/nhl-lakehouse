package window

import (
	"fmt"
	"time"
)

const dateLayout = "2006-01-02"

// Dates returns an inclusive rolling date window based on the local calendar
// date at now. The clock is supplied by the caller so boundary behavior can
// be tested without depending on wall-clock time.
func Dates(now time.Time, location *time.Location, lookbackDays, lookaheadDays int) (time.Time, time.Time, error) {
	if location == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("timezone must not be nil")
	}
	if lookbackDays < 0 || lookaheadDays < 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("lookback and lookahead days must be non-negative")
	}
	localNow := now.In(location)
	localDate := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, location)
	return localDate.AddDate(0, 0, -lookbackDays), localDate.AddDate(0, 0, lookaheadDays), nil
}

func FormatDate(t time.Time) string { return t.Format(dateLayout) }
