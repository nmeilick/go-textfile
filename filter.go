package textfile

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/sync/errgroup"
)

// FilterFunc signature
type FilterFunc func(io.Reader, io.Writer) error

// Filter reads data from a reader, transforms it with filter functions and
// writes the resulting data to a writer. Filter functions are applied in the
// specified order.
func Filter(r io.Reader, w io.Writer, ff ...FilterFunc) error {
	// Shortcuts if less than 2 filter were specified
	switch len(ff) {
	case 0:
		_, err := io.Copy(w, r)
		return err
	case 1:
		return ff[0](r, w)
	}

	// Run filters concurrently in a pipeline
	g := errgroup.Group{}
	readers := make([]*io.PipeReader, len(ff)-1)
	writers := make([]*io.PipeWriter, len(readers))
	for i := 0; i < len(readers); i++ {
		readers[i], writers[i] = io.Pipe()
	}

	for i := range ff {
		i := i
		g.Go(func() error {
			var err error
			switch {
			case i == 0:
				err = ff[0](r, writers[0])
				writers[0].Close()
			case i == len(ff)-1:
				err = ff[i](readers[i-1], w)
				readers[i-1].Close()
			default:
				err = ff[i](readers[i-1], writers[i])
				writers[i].Close()
				readers[i-1].Close()
			}
			return err
		})
	}
	return g.Wait()
}

type MapFunc func([]byte) ([]byte, error)

// Map returns a filter function that reads lines from a reader, transforms them with
// the given map functions, and writes the result to a writer.
func Map(ff ...MapFunc) FilterFunc {
	return func(r io.Reader, w io.Writer) error {
		// Shortcut if no mapping functions are given
		if len(ff) == 0 {
			_, err := io.Copy(w, r)
			return err
		}

		s := bufio.NewScanner(r)
		var err error
		for s.Scan() {
			l := append([]byte(nil), s.Bytes()...)
			for _, f := range ff {
				l, err = f(l)
				if err != nil {
					return err
				}
				if l == nil {
					break
				}
			}
			if l != nil {
				if _, err = w.Write(append(l, '\n')); err != nil {
					return err
				}
			}
		}
		return s.Err()
	}
}

// Join returns a filter function that reads lines from a reader, removes trailing whitespace,
// joins lines marked with the continuation character (\) and writes the result to a writer.
// Line numbers are preserved by inserting newlines after continued lines.
func Join() FilterFunc {
	return func(r io.Reader, w io.Writer) error {
		s := bufio.NewScanner(r)
		var lr, lw int
		var err error
		for ; s.Scan(); lr++ {
			l := append([]byte(nil), s.Bytes()...)
			// Remove trailing whitespace
			l = bytes.TrimRightFunc(l, unicode.IsSpace)

			if i := len(l) - 1; i >= 0 && l[i] == '\\' {
				if i > 1 {
					// Write continued line without trailing \
					_, err = w.Write(l[:i])
				}
			} else {
				// Add as many newlines as needed to ensure line numbers do not change for non-continuation lines.
				l = append(l, bytes.Repeat([]byte("\n"), 1+lr-lw)...)
				_, err = w.Write(l)
				lw = lr + 1
			}
			if err != nil {
				return err
			}
		}
		if err = s.Err(); err == nil && lw != lr {
			// Last line is a continuation line, end with a newline.
			_, err = w.Write([]byte("\n"))
		}
		return err
	}
}

// Grep returns a filter function that only let's through matching lines.
// If invert is true, only non-matching lines are returned.
func Grep(re *regexp.Regexp, invert bool) FilterFunc {
	return Map(func(l []byte) ([]byte, error) {
		match := re.Match(l)
		if invert {
			match = !match
		}
		if match {
			return l, nil
		}
		return nil, nil
	})
}

// Format returns a filter function that transforms a line by using
// fmt.Sprintf(format, string(line))
func Format(format string) FilterFunc {
	return Map(func(l []byte) ([]byte, error) {
		return []byte(fmt.Sprintf(format, string(l))), nil
	})
}

type LookupFunc func(string) (string, bool)

// Format returns a filter function that transforms a line by using
// fmt.Sprintf(format, string(line))
func Expand(re *regexp.Regexp, ff ...LookupFunc) FilterFunc {
	idx := re.SubexpIndex("name")
	if len(ff) == 0 {
		panic("no lookup function specified")
	}

	return Map(func(l []byte) ([]byte, error) {
		if idx == -1 {
			return nil, errors.New("regexp is missing named parenthesized subexpression (?P<name>...): " + re.String())
		}
		matches := re.FindAllSubmatchIndex(l, -1)
		if len(matches) == 0 {
			return l, nil
		}

		l2 := []byte{}
		pos := 0
		for _, m := range matches {
			var val []byte
			name := string(l[m[idx*2]:m[idx*2+1]])
			for _, f := range ff {
				if s, ok := f(name); ok {
					val = []byte(s)
					break
				}
			}
			if val == nil {
				return nil, errors.New("could not resolve variable: " + name)
			}
			l2 = append(l2, l[pos:m[0]]...)
			l2 = append(l2, []byte(val)...)
			pos = m[1]
		}
		l2 = append(l2, l[pos:len(l)]...)
		return l2, nil
	})
}

// LookupMap returns a lookup function that uses the given map as data source.
func LookupMap(m map[string]string) LookupFunc {
	if m == nil {
		m = map[string]string{}
	}
	return func(name string) (string, bool) {
		val, found := m[name]
		return val, found
	}
}

// LookupEnv returns a lookup function that uses the current environment as data source.
func LookupEnv() LookupFunc {
	return func(name string) (string, bool) {
		if val := os.Getenv(name); val != "" {
			return val, true
		}
		return "", false
	}
}

// LookupStatic returns a lookup function that returns the given value.
// If the value contains %s, all occurences will be replaced by the
// name that is looked up.
func LookupStatic(val string) LookupFunc {
	return func(name string) (string, bool) {
		if strings.Contains(val, "%s") {
			return strings.ReplaceAll(val, "%s", name), true
		}
		return val, true
	}
}
