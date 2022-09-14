package cmn

import (
	"fmt"
	"log"
	"reflect"
	"regexp"
	"sort"

	"golang.org/x/xerrors"
)

const (
	cidTrimPrefix = 6
	cidTrimSuffix = 8
)

func TrimCidString(cs string) string { //nolint:revive
	if len(cs) <= cidTrimPrefix+cidTrimSuffix+2 {
		return cs
	}
	return cs[0:cidTrimPrefix] + "~" + cs[len(cs)-cidTrimSuffix:]
}

var NonAlphanumRun = regexp.MustCompile(`[^a-zA-Z0-9]+`) //nolint:revive

func SortedMapKeys(m interface{}) []string { //nolint:revive
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		log.Panicf("input type not a map: %v", v)
	}
	avail := make([]string, 0, v.Len())
	for _, k := range v.MapKeys() {
		avail = append(avail, k.String())
	}
	sort.Strings(avail)
	return avail
}

type cmnErr struct {
	err   error
	frame xerrors.Frame
}

var _ error = &cmnErr{}
var _ fmt.Formatter = &cmnErr{}
var _ xerrors.Formatter = &cmnErr{}
var _ xerrors.Wrapper = &cmnErr{}

func WrErr(err error) error { //nolint:revive
	if err == nil {
		return nil
	}
	return &cmnErr{err: err, frame: xerrors.Caller(1)}
}
func (e *cmnErr) Unwrap() error              { return e.err }
func (e *cmnErr) Error() string              { return fmt.Sprint(e) }
func (e *cmnErr) Format(s fmt.State, v rune) { xerrors.FormatError(e, s, v) }
func (e *cmnErr) FormatError(p xerrors.Printer) error {
	p.Print(e.err.Error())
	if p.Detail() {
		e.frame.Format(p)
	}
	return nil
}
