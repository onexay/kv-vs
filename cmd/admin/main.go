package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
)

const (
	defaultAPI = "http://localhost:8080"
)

type policyResponse struct {
	Name           string `json:"name"`
	HotCommitLimit int    `json:"hotCommitLimit"`
	HotDuration    string `json:"hotDuration"`
	Locked         bool   `json:"locked"`
}

func main() {
	api := flag.String("api", envDefault("KVVS_API", defaultAPI), "Base URL of the kv-vs REST API")
	repo := flag.String("repo", "", "Repository name (required)")
	dumpJSON := flag.Bool("json", false, "Output JSON instead of table")
	flag.Parse()

	if *repo == "" {
		fmt.Fprintln(os.Stderr, "--repo is required")
		os.Exit(1)
	}

	query := url.Values{"name": {*repo}}
	endpoint := fmt.Sprintf("%s/api/v1/policies?%s", strings.TrimRight(*api, "/"), query.Encode())

	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build request: %v\n", err)
		os.Exit(1)
	}

	req.Header.Set("X-Author-Name", "admin")
	req.Header.Set("X-Author-ID", "admin-cli")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "policy query failed: %s\n", resp.Status)
		os.Exit(1)
	}

	var policy policyResponse
	if err := json.NewDecoder(resp.Body).Decode(&policy); err != nil {
		fmt.Fprintf(os.Stderr, "decode response: %v\n", err)
		os.Exit(1)
	}

	if *dumpJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(policy)
		return
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Repo\tHotLimit\tHotDuration\tLocked\n")
	fmt.Fprintf(tw, "%s\t%d\t%s\t%t\n", policy.Name, policy.HotCommitLimit, policy.HotDuration, policy.Locked)
	_ = tw.Flush()
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
