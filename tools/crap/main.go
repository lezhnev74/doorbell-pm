// Command crap computes the CRAP score (Change Risk Anti-Patterns) of every
// function in a Go coverage profile and fails when any function is above the
// threshold or when total statement coverage is below the minimum.
//
//	CRAP(f) = complexity(f)^2 * (1 - coverage(f))^3 + complexity(f)
//
// Complexity is cyclomatic, counted the same way gocyclo does.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type fn struct {
	file       string
	line       int
	name       string
	coverage   float64 // 0..1
	complexity int
}

func (f fn) crap() float64 {
	c := float64(f.complexity)
	u := 1 - f.coverage
	return c*c*u*u*u + c
}

func main() {
	profile := flag.String("profile", "cover.out", "coverage profile produced by go test -coverprofile")
	maxCrap := flag.Float64("max", 6, "maximum allowed CRAP score per function")
	minCov := flag.Float64("min-cov", 85, "minimum total statement coverage in percent")
	all := flag.Bool("all", false, "print every function, not only the offenders")
	flag.Parse()

	modPath, modDir, err := module()
	if err != nil {
		fatal(err)
	}
	fns, total, err := coverFunc(*profile, modPath, modDir)
	if err != nil {
		fatal(err)
	}
	if err := complexity(fns); err != nil {
		fatal(err)
	}

	sort.Slice(fns, func(i, j int) bool { return fns[i].crap() > fns[j].crap() })
	failed := false
	for _, f := range fns {
		over := f.crap() > *maxCrap
		if over {
			failed = true
		}
		if over || *all {
			fmt.Printf("%-7.1f cyclo=%-3d cov=%5.1f%%  %s:%d %s\n",
				f.crap(), f.complexity, f.coverage*100, rel(f.file, modDir), f.line, f.name)
		}
	}
	fmt.Printf("total coverage: %.1f%% (min %.1f%%), functions: %d, CRAP > %.1f: %d\n",
		total, *minCov, len(fns), *maxCrap, countOver(fns, *maxCrap))
	if total < *minCov {
		failed = true
	}
	if failed {
		os.Exit(1)
	}
}

func countOver(fns []fn, max float64) int {
	n := 0
	for _, f := range fns {
		if f.crap() > max {
			n++
		}
	}
	return n
}

func module() (path, dir string, err error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Path}}\t{{.Dir}}").Output()
	if err != nil {
		return "", "", fmt.Errorf("go list -m: %w", err)
	}
	parts := strings.SplitN(strings.TrimSpace(string(out)), "\t", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("go list -m: unexpected output %q", out)
	}
	return parts[0], parts[1], nil
}

// coverFunc parses the output of `go tool cover -func`, which lists one
// function per line as "<import path>/<file>:<line>:\t<name>\t<pct>%" and ends
// with a "total:" line.
func coverFunc(profile, modPath, modDir string) ([]fn, float64, error) {
	out, err := exec.Command("go", "tool", "cover", "-func="+profile).Output()
	if err != nil {
		return nil, 0, fmt.Errorf("go tool cover -func: %w", err)
	}
	var fns []fn
	total := -1.0
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 {
			continue
		}
		pct, err := strconv.ParseFloat(strings.TrimSuffix(fields[len(fields)-1], "%"), 64)
		if err != nil {
			return nil, 0, fmt.Errorf("parse %q: %w", sc.Text(), err)
		}
		if fields[0] == "total:" {
			total = pct
			continue
		}
		loc := strings.Split(fields[0], ":")
		if len(loc) < 2 {
			return nil, 0, fmt.Errorf("parse location %q", fields[0])
		}
		line, err := strconv.Atoi(loc[1])
		if err != nil {
			return nil, 0, fmt.Errorf("parse line %q: %w", fields[0], err)
		}
		file := strings.TrimPrefix(loc[0], modPath+"/")
		fns = append(fns, fn{
			file:     filepath.Join(modDir, filepath.FromSlash(file)),
			line:     line,
			name:     fields[1],
			coverage: pct / 100,
		})
	}
	if total < 0 {
		return nil, 0, fmt.Errorf("no total line in cover -func output")
	}
	return fns, total, nil
}

// complexity parses every file referenced by fns and fills in the cyclomatic
// complexity of the function declared at each recorded line.
func complexity(fns []fn) error {
	byFile := map[string][]*fn{}
	for i := range fns {
		byFile[fns[i].file] = append(byFile[fns[i].file], &fns[i])
	}
	fset := token.NewFileSet()
	for file, list := range byFile {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			return err
		}
		byLine := map[int]int{}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			byLine[fset.Position(fd.Pos()).Line] = cyclomatic(fd)
		}
		for _, fn := range list {
			c, ok := byLine[fn.line]
			if !ok {
				return fmt.Errorf("%s:%d: no function declaration for %s", file, fn.line, fn.name)
			}
			fn.complexity = c
		}
	}
	return nil
}

func cyclomatic(fd *ast.FuncDecl) int {
	c := 1
	ast.Inspect(fd, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt:
			c++
		case *ast.CaseClause:
			if n.List != nil { // default clause does not branch
				c++
			}
		case *ast.CommClause:
			if n.Comm != nil {
				c++
			}
		case *ast.BinaryExpr:
			if n.Op == token.LAND || n.Op == token.LOR {
				c++
			}
		}
		return true
	})
	return c
}

func rel(file, dir string) string {
	if r, err := filepath.Rel(dir, file); err == nil {
		return r
	}
	return file
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "crap:", err)
	os.Exit(2)
}
