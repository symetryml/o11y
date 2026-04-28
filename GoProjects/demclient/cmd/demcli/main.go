// Command demcli is a CLI for the SymetryML DEM REST API.
//
// Usage:
//
//	demcli list
//	demcli create <project> [--type cpu] [--persist]
//	demcli info <project>
//	demcli stream <project> <file.csv> [--chunk 1000]
//	demcli stream <project> --stdin [--chunk 1000]
//	demcli explore <project> <variable>
//	demcli explore <project> <var1> <var2>
//	demcli histogram <project> <variable> [--bins 20]
//
// Configuration via environment variables:
//
//	SML_SERVER       DEM server URL (default: http://localhost:8080)
//	SML_KEY_ID       API key ID
//	SML_SECRET_KEY   Base64-encoded secret key
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/symetryml/demclient"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	client := demclient.NewClient(configFromEnv())
	cmd := os.Args[1]

	switch cmd {
	case "list":
		cmdList(client)
	case "create":
		cmdCreate(client, os.Args[2:])
	case "info":
		cmdInfo(client, os.Args[2:])
	case "stream":
		cmdStream(client, os.Args[2:])
	case "explore":
		cmdExplore(client, os.Args[2:])
	case "histogram":
		cmdHistogram(client, os.Args[2:])
	case "delete":
		cmdDelete(client, os.Args[2:])
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func configFromEnv() demclient.Config {
	server := os.Getenv("SML_SERVER")
	if server == "" {
		server = "http://localhost:8080"
	}
	return demclient.Config{
		Server:       server,
		SymKeyID:     os.Getenv("SML_KEY_ID"),
		SymSecretKey: os.Getenv("SML_SECRET_KEY"),
		ClientID:     "demcli",
	}
}

func cmdList(c *demclient.Client) {
	projects, err := c.ListProjects()
	fatal(err)
	if len(projects) == 0 {
		fmt.Println("No projects found.")
		return
	}
	for _, p := range projects {
		fmt.Println(p)
	}
}

func cmdCreate(c *demclient.Client, args []string) {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	pType := fs.String("type", "cpu", "Project type")
	persist := fs.Bool("persist", true, "Persist project to disk")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: demcli create <project> [--type cpu] [--persist]")
		os.Exit(1)
	}

	pid := fs.Arg(0)
	err := c.CreateProject(pid, *pType, *persist)
	fatal(err)
	fmt.Printf("Project %q created.\n", pid)
}

func cmdInfo(c *demclient.Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: demcli info <project>")
		os.Exit(1)
	}

	resp, err := c.GetProjectInfo(args[0])
	fatal(err)
	printJSON(resp.Values)
}

func cmdDelete(c *demclient.Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: demcli delete <project>")
		os.Exit(1)
	}

	err := c.DeleteProject(args[0])
	fatal(err)
	fmt.Printf("Project %q deleted.\n", args[0])
}

func cmdStream(c *demclient.Client, args []string) {
	fs := flag.NewFlagSet("stream", flag.ExitOnError)
	stdin := fs.Bool("stdin", false, "Read CSV from stdin")
	chunk := fs.Int("chunk", 1000, "Rows per API call")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: demcli stream <project> <file.csv> [--chunk 1000]")
		fmt.Fprintln(os.Stderr, "       demcli stream <project> --stdin [--chunk 1000]")
		os.Exit(1)
	}

	pid := fs.Arg(0)

	var reader *os.File
	if *stdin {
		reader = os.Stdin
	} else {
		if fs.NArg() < 2 {
			fmt.Fprintln(os.Stderr, "Error: provide a CSV file path or use --stdin")
			os.Exit(1)
		}
		f, err := os.Open(fs.Arg(1))
		fatal(err)
		defer f.Close()
		reader = f
	}

	n, err := c.StreamCSV(pid, reader, *chunk)
	fatal(err)
	fmt.Printf("Streamed %d rows to project %q.\n", n, pid)
}

func cmdExplore(c *demclient.Client, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: demcli explore <project> <variable> [<variable2>]")
		os.Exit(1)
	}

	pid := args[0]

	if len(args) == 2 {
		// Univariate
		stats, err := c.GetUnivariateStats(pid, args[1])
		fatal(err)
		printMap(stats)
	} else {
		// Bivariate
		stats, err := c.GetBivariateStats(pid, args[1], args[2])
		fatal(err)
		printMap(stats)
	}
}

func cmdHistogram(c *demclient.Client, args []string) {
	fs := flag.NewFlagSet("histogram", flag.ExitOnError)
	bins := fs.Int("bins", 20, "Number of histogram bins")
	fs.Parse(args)

	if fs.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "Usage: demcli histogram <project> <variable> [--bins 20]")
		os.Exit(1)
	}

	pid := fs.Arg(0)
	variable := fs.Arg(1)

	resp, err := c.GetHistogram(pid, variable, *bins)
	fatal(err)
	printJSON(resp.Values)
}

func printUsage() {
	usage := `demcli - SymetryML DEM REST API CLI

Usage:
  demcli list                                     List all projects
  demcli create <project> [--type cpu] [--persist] Create project
  demcli info <project>                           Project info
  demcli delete <project>                         Delete project
  demcli stream <project> <file.csv> [--chunk N]  Stream CSV to project
  demcli stream <project> --stdin [--chunk N]     Stream from stdin
  demcli explore <project> <var>                  Univariate stats
  demcli explore <project> <var1> <var2>          Bivariate stats
  demcli histogram <project> <var> [--bins N]     Density estimation

Environment:
  SML_SERVER       DEM server URL (default: http://localhost:8080)
  SML_KEY_ID       API key ID
  SML_SECRET_KEY   Base64-encoded secret key`
	fmt.Println(usage)
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printJSON(data json.RawMessage) {
	var pretty any
	if err := json.Unmarshal(data, &pretty); err != nil {
		fmt.Println(string(data))
		return
	}
	out, _ := json.MarshalIndent(pretty, "", "  ")
	fmt.Println(string(out))
}

func printMap(m map[string]any) {
	// Find max key length for alignment
	maxLen := 0
	for k := range m {
		if len(k) > maxLen {
			maxLen = len(k)
		}
	}

	// Sort keys and print
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)

	for _, k := range keys {
		v := m[k]
		padding := strings.Repeat(" ", maxLen-len(k))
		switch val := v.(type) {
		case float64:
			fmt.Printf("  %s%s  %s\n", k, padding, strconv.FormatFloat(val, 'g', 6, 64))
		default:
			fmt.Printf("  %s%s  %v\n", k, padding, v)
		}
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
