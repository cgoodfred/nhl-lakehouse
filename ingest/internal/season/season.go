package season

import (
	"fmt"
	"strconv"
	"time"
)

func Dates(season string) (time.Time, time.Time, error) {
	if len(season) != 8 {
		return time.Time{}, time.Time{}, fmt.Errorf("season must be 8 digits, got %q", season)
	}
	start, err := strconv.Atoi(season[:4])
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	end, err := strconv.Atoi(season[4:])
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	if end-start != 1 {
		return time.Time{}, time.Time{}, fmt.Errorf("start and end must be consecutive years")
	}
	return time.Date(start, time.October, 1, 0, 0, 0, 0, time.UTC), time.Date(end, time.July, 1, 0, 0, 0, 0, time.UTC), nil
}
