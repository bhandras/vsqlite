package main

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	prompt "github.com/c-bata/go-prompt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/ktr0731/go-fuzzyfinder"
	_ "github.com/mattn/go-sqlite3"
)

const (
	// The delimiter to use in the history file.
	customHistoryDelimiter = "---"
)

// Our table style.
var psqlStyle = table.Style{
	Name: "psql-style",
	Box: table.BoxStyle{
		BottomLeft:       "+",
		BottomRight:      "+",
		BottomSeparator:  "+",
		Left:             "|",
		LeftSeparator:    "+",
		MiddleHorizontal: "-",
		MiddleSeparator:  "+",
		MiddleVertical:   "|",
		PaddingLeft:      " ",
		PaddingRight:     " ",
		Right:            "|",
		RightSeparator:   "+",
		TopLeft:          "+",
		TopRight:         "+",
		TopSeparator:     "+",
		UnfinishedRow:    "…",
	},
	Color: table.ColorOptionsDefault,
	Format: table.FormatOptions{
		Header: text.FormatLower, // or text.FormatDefault
	},
	Options: table.Options{
		DrawBorder:      true,
		SeparateColumns: true,
		SeparateHeader:  true,
		SeparateRows:    false,
	},
}

var (
	db           *sql.DB
	expandedMode bool
	historyFile  string
	historyLines []string
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: sqlite-client <database-file>")
		os.Exit(1)
	}
	dbPath := os.Args[1]

	var err error
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	historyFile = getHistoryFilePath()
	loadHistory()

	fmt.Println("Enter SQL statements. Type 'exit' to quit. Use '\\x' to " +
		"toggle expanded mode.")

	p := prompt.New(
		executor,
		completer,
		prompt.OptionPrefix("sqlite> "),
		prompt.OptionTitle("sqlite-client"),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ControlR,
			Fn: func(buf *prompt.Buffer) {
				selected := fuzzyHistoryPrompt()
				if selected != "" {
					buf.DeleteBeforeCursor(
						len(buf.Document().
							TextBeforeCursor()),
					)
					buf.InsertText(selected, false, false)
				}
			},
		}),
	)

	p.Run()
	saveHistory()
}

func executor(input string) {
	// Make sure that we don't execute empty queries.
	query := strings.TrimSpace(input)
	if query == "" {
		return
	}

	saveToHistory(query)

	switch {
	case query == "exit":
		os.Exit(0)

	case query == `\x`:
		expandedMode = !expandedMode
		expandedModeStr := "off"
		if expandedMode {
			expandedModeStr = "on"
		}

		fmt.Printf("Expanded display is now %v\n", expandedModeStr)
		return

	case strings.HasPrefix(query, ".schema"):
		handleSchemaCommand(query)
		return
	}

	rows, err := db.Query(query)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}
	defer rows.Close()

	if expandedMode {
		hasRows, err := printExpanded(rows)
		if err != nil {
			fmt.Printf("Error printing expanded: %v\n", err)
			return
		}

		if !hasRows {
			fmt.Println("No rows found.")
		}
	} else {
		printPrettyTable(rows)
	}
}

func completer(d prompt.Document) []prompt.Suggest {
	suggestTables := func(prefixIdx int) func([]string) []prompt.Suggest {
		return func(m []string) []prompt.Suggest {
			return prompt.FilterHasPrefix(
				getTableSuggestions(), m[prefixIdx], true,
			)
		}
	}

	suggestColumns := func(tableIdx,
		colPrefixIdx int) func([]string) []prompt.Suggest {

		return func(m []string) []prompt.Suggest {
			return prompt.FilterHasPrefix(
				getColumnSuggestions(m[tableIdx]),
				m[colPrefixIdx], true,
			)

		}
	}

	type rule struct {
		pattern *regexp.Regexp
		handler func([]string) []prompt.Suggest
	}

	rules := []rule{
		// .schema [table]

		{
			regexp.MustCompile(`(?i)^\.schema\s+(\w*)$`),
			suggestTables(1),
		},

		// table.column
		{
			regexp.MustCompile(`(?i)(\w+)\.(\w*)$`),
			suggestColumns(1, 2),
		},

		// SELECT ... FROM <table>
		{
			regexp.MustCompile(`(?i)\bSELECT\b.*\bFROM\s+(\w*)$`),
			suggestTables(1),
		},

		// INSERT INTO <table>
		{
			regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+(\w*)$`),
			suggestTables(1),
		},

		// UPDATE <table> SET ...
		{

			regexp.MustCompile(`(?i)^UPDATE(?:\s+OR\s+(?:ROLLBACK|ABORT|REPLACE|FAIL|IGNORE))?\s+(\w+)\s+SET\s+(?:[^=]+=\s*[^,]+,\s*)*(\w*)$`),
			suggestColumns(1, 2),
		},

		// UPDATE <table>
		{
			regexp.MustCompile(`(?i)\bUPDATE\s+(\w*)$`),
			suggestTables(1),
		},

		// FROM or JOIN <table>
		{
			regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+(\w*)$`),
			suggestTables(1),
		},
	}

	text := d.TextBeforeCursor()
	for _, r := range rules {
		if m := r.pattern.FindStringSubmatch(text); m != nil {
			return r.handler(m)
		}
	}

	return nil
}

func handleSchemaCommand(query string) {
	args := strings.Fields(query)
	if len(args) == 1 {
		rows, err := db.Query(`SELECT sql FROM sqlite_master
			               WHERE type='table'`)
		if err != nil {
			fmt.Println("Schema query failed:", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var sqlStmt string
			rows.Scan(&sqlStmt)
			fmt.Println(sqlStmt)
		}
	} else {
		table := args[1]
		row := db.QueryRow(`SELECT sql FROM sqlite_master
			            WHERE type='table' AND name=?`, table)
		var sqlStmt string
		err := row.Scan(&sqlStmt)
		if err != nil {
			fmt.Println("No such table.")
			return
		}

		fmt.Println(sqlStmt)
	}
}

func getTableSuggestions() []prompt.Suggest {
	rows, _ := db.Query(`SELECT name FROM sqlite_master
		             WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	defer rows.Close()

	var suggestions []prompt.Suggest
	for rows.Next() {
		var name string
		rows.Scan(&name)
		suggestions = append(
			suggestions,
			prompt.Suggest{Text: name, Description: "table"},
		)
	}
	return suggestions
}

func getColumnSuggestions(table string) []prompt.Suggest {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil
	}
	defer rows.Close()

	var suggestions []prompt.Suggest
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dfltValue sql.NullString
		rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk)
		suggestions = append(
			suggestions,
			prompt.Suggest{Text: name, Description: "column"},
		)
	}
	return suggestions
}

func formatValue(val interface{}) string {
	switch v := val.(type) {
	case nil:
		return "NULL"

	case []byte:
		return `\x` + strings.ToUpper(hex.EncodeToString(v))

	default:
		return fmt.Sprintf("%v", v)
	}
}

func isNumeric(s string) bool {
	_, err := fmt.Sscanf(s, "%f", new(float64))
	return err == nil
}

func printPrettyTable(rows *sql.Rows) {
	cols, _ := rows.Columns()
	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(psqlStyle)
	t.Style().Format.Header = text.FormatLower
	t.AppendHeader(toRow(cols))

	var sampleRow []string
	var columnConfigs []table.ColumnConfig

	// Scan one row to guess column types.
	if rows.Next() {
		rows.Scan(valPtrs...)
		row := make([]interface{}, len(cols))
		sampleRow = make([]string, len(cols))

		for i, val := range vals {
			s := formatValue(val)
			row[i] = s
			sampleRow[i] = s
		}
		t.AppendRow(row)
	}

	// Determine right-aligned columns (numeric heuristics).
	for i, val := range sampleRow {
		if isNumeric(val) {
			columnConfigs = append(
				columnConfigs, table.ColumnConfig{
					Number: i + 1, Align: text.AlignRight,
				},
			)
		}
	}
	t.SetColumnConfigs(columnConfigs)

	// Continue with the rest of the rows.
	for rows.Next() {
		rows.Scan(valPtrs...)
		row := make([]interface{}, len(cols))
		for i, val := range vals {
			row[i] = formatValue(val)
		}
		t.AppendRow(row)
	}

	t.Render()
}

func printExpanded(rows *sql.Rows) (bool, error) {
	cols, err := rows.Columns()
	if err != nil {
		fmt.Printf("Failed to get columns: %v\n", err)
		return false, err
	}

	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}

	type rowData []string
	var allData []rowData

	maxKeyLen := 0

	// Scan rows into memory to determine max key length.
	for rows.Next() {
		if err := rows.Scan(valPtrs...); err != nil {
			fmt.Printf("Failed to scan row: %v\n", err)
			return false, err
		}
		row := make(rowData, len(cols))
		for i, val := range vals {
			row[i] = formatValue(val)
		}
		allData = append(allData, row)
	}

	if len(allData) == 0 {
		return false, nil
	}

	// Find max key width.
	for _, col := range cols {
		if len(col) > maxKeyLen {
			maxKeyLen = len(col)
		}
	}

	// Calculate the max digits to use for the record number.
	digitCount := int(math.Log10(float64(len(allData)))) + 1

	// Print all rows.
	for i, row := range allData {
		fmt.Printf("-[ RECORD %*d ]%s\n", digitCount, i+1,
			strings.Repeat("-", 24))

		for j, col := range cols {
			fmt.Printf("%-*s | %s\n", maxKeyLen, col, row[j])
		}
		fmt.Println()
	}

	return true, nil
}

func toRow(cols []string) table.Row {
	row := make(table.Row, len(cols))
	for i, col := range cols {
		row[i] = col
	}
	return row
}

func getHistoryFilePath() string {
	usr, _ := user.Current()
	return filepath.Join(usr.HomeDir, ".vsqlite_history")
}

func unescapeHistoryLines(lines []string) []string {
	var out []string
	for _, line := range lines {
		unquoted, err := strconv.Unquote(`"` + line + `"`)
		if err == nil {
			out = append(out, unquoted)
		} else {
			out = append(out, line)
		}
	}
	return out
}

func saveToHistory(cmd string) {
	historyLines = append(historyLines, cmd)
}

func loadHistory() {
	file, err := os.Open(historyFile)
	if err != nil {
		return
	}
	defer file.Close()

	var block []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if line == customHistoryDelimiter {
			if len(block) > 0 {
				historyLines = append(
					historyLines, strings.Join(block, "\n"),
				)
				block = nil
			}
			continue
		}
		block = append(block, line)
	}
	if len(block) > 0 {
		historyLines = append(historyLines, strings.Join(block, "\n"))
	}

	historyLines = dedupHistory(historyLines)
}

func dedupHistory(lines []string) []string {
	seen := make(map[string]int)
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		if _, exists := seen[line]; !exists {
			seen[line] = i
		}
	}

	// Rebuild from most recent to oldest (reverse index order).
	ordered := make([]string, 0, len(seen))
	indices := make([]int, 0, len(seen))
	for _, idx := range seen {
		indices = append(indices, idx)
	}

	sort.Ints(indices)
	for _, idx := range indices {
		ordered = append(ordered, lines[idx])
	}

	return ordered
}

func saveHistory() {
	if len(historyLines) == 0 {
		return
	}
	f, err := os.OpenFile(
		historyFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644,
	)
	if err != nil {
		return
	}
	defer f.Close()

	for _, entry := range historyLines {
		fmt.Fprintln(f, customHistoryDelimiter)
		f.WriteString(entry)
		if !strings.HasSuffix(entry, "\n") {
			f.WriteString("\n")
		}
	}
}

func fuzzyHistoryPrompt() string {
	if len(historyLines) == 0 {
		return ""
	}

	idx, err := fuzzyfinder.Find(
		historyLines,
		func(i int) string {
			return historyLines[i]
		},
		fuzzyfinder.WithPromptString("🔍 history> "),
	)
	if err != nil {
		// User cancelled or no selection.
		return ""
	}
	return historyLines[idx]
}
