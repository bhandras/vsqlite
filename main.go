package main

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	prompt "github.com/c-bata/go-prompt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/ktr0731/go-fuzzyfinder"
	_ "modernc.org/sqlite"
)

const (
	// The delimiter to use in the history file.
	customHistoryDelimiter = "---"
)

// Our table style.
var psqlStyle = table.Style{
	Name: "psql",
	Box: table.BoxStyle{
		BottomLeft:       "",
		BottomRight:      "",
		BottomSeparator:  "",
		Left:             "|",
		LeftSeparator:    "+",
		MiddleHorizontal: "-",
		MiddleSeparator:  "+",
		MiddleVertical:   "|",
		PaddingLeft:      " ",
		PaddingRight:     " ",
		Right:            "|",
		RightSeparator:   "+",
		TopLeft:          "",
		TopRight:         "",
		TopSeparator:     "",
		UnfinishedRow:    "…",
	},
	Color: table.ColorOptionsDefault,
	Format: table.FormatOptions{
		Header: text.FormatLower,
	},
	Options: table.Options{
		DrawBorder:      false,
		SeparateColumns: true,
		SeparateHeader:  true,
		SeparateRows:    false,
	},
}

var (
	db           *sql.DB
	expandedMode bool
	jsonMode     bool
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
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	historyFile = getHistoryFilePath()
	loadHistory()

	fmt.Println(
		`Enter SQL statements. Built-in commands:
		    \x         → toggle expanded display
		    \j         → toggle JSON output
		    \d [table] → show table schema
		    \d         → list all tables/views
		    \di        → list all indexes
		    CTRL+D     → quit`,
	)

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

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
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
		if expandedMode {
			jsonMode = false
		}
		fmt.Printf("Expanded display is now %s\n", onOff(expandedMode))

		return

	case query == `\j`:
		jsonMode = !jsonMode
		if jsonMode {
			expandedMode = false
		}
		fmt.Printf("JSON output is now %s\n", onOff(jsonMode))

		return

	case strings.HasPrefix(query, `\d `):
		table := strings.TrimSuffix(
			strings.TrimPrefix(query, `\d `), ";",
		)

		if table == "" {
			fmt.Println("Usage: \\d <table>")
			return
		}

		if err := printSchemaPretty(table); err != nil {
			fmt.Printf("Schema error: %v\n", err)
		}

		return

	case strings.TrimSpace(query) == `\d` || strings.TrimSpace(query) == `\d;`:
		if err := printRelationList(); err != nil {
			fmt.Printf("Error: %v\n", err)
		}

		return

	case strings.TrimSpace(query) == `\di` || strings.TrimSpace(query) == `\di;`:
		if err := printIndexList(); err != nil {
			fmt.Printf("Error: %v\n", err)
		}

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
	} else if jsonMode {
		if err := printJSON(rows); err != nil {
			fmt.Printf("JSON output error: %v\n", err)
		}
		return
	} else {
		err := printPrettyTable(rows)
		if err != nil {
			fmt.Printf("Error printing table: %v\n", err)
			return
		}
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
		// \d [table]
		{
			regexp.MustCompile(`(?i)^\\d\s+(\w+)$`),
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

func printRelationList() error {
	rows, err := db.Query(`
		SELECT name, type
		FROM sqlite_master
		WHERE type IN ('table', 'view')
		  AND name NOT LIKE 'sqlite_%'
		ORDER BY type DESC, name;
	`)
	if err != nil {
		return fmt.Errorf("failed to list relations: %w", err)
	}
	defer rows.Close()

	fmt.Println("        List of relations")
	fmt.Printf(" %-32s | %-6s\n", "Name", "Type")
	fmt.Println(strings.Repeat("-", 41))

	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return err
		}
		fmt.Printf(" %-32s | %-6s\n", name, typ)
	}
	return nil
}

func printIndexList() error {
	rows, err := db.Query(`
		SELECT name, tbl_name
		FROM sqlite_master
		WHERE type = 'index'
		  AND name NOT LIKE 'sqlite_%'
		ORDER BY tbl_name, name;
	`)
	if err != nil {
		return fmt.Errorf("failed to list indexes: %w", err)
	}
	defer rows.Close()

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(psqlStyle)
	t.AppendHeader(table.Row{"Index Name", "Table"})

	for rows.Next() {
		var name, tbl string
		if err := rows.Scan(&name, &tbl); err != nil {
			return err
		}
		t.AppendRow(table.Row{name, tbl})
	}

	t.Render()
	return nil
}

func printSchemaPretty(tableName string) error {
	fmt.Printf("\n📄 Table \"%s\"\n\n", tableName)

	// Columns
	colRows, err := db.Query(
		fmt.Sprintf("PRAGMA table_info(%q)", tableName),
	)
	if err != nil {
		return fmt.Errorf("PRAGMA table_info: %w", err)
	}
	defer colRows.Close()

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(psqlStyle)
	t.AppendHeader(
		table.Row{"Column", "Type", "Collation", "Nullable", "Default"},
	)

	for colRows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		colRows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk)

		nullable := "yes"
		if notnull != 0 {
			nullable = "no"
		}
		defaultVal := ""
		if dflt.Valid {
			defaultVal = dflt.String
		}

		t.AppendRow(table.Row{name, ctype, "", nullable, defaultVal})
	}
	t.Render()

	// Indexes
	idxRows, err := db.Query(fmt.Sprintf("PRAGMA index_list(%q)", tableName))
	if err != nil {
		return err
	}
	defer idxRows.Close()

	idxTable := table.NewWriter()
	idxTable.SetOutputMirror(os.Stdout)
	idxTable.SetStyle(psqlStyle)
	idxTable.AppendHeader(table.Row{"Index Name", "Details"})

	for idxRows.Next() {
		var seq int
		var name string
		var unique int
		var origin, partial string
		idxRows.Scan(&seq, &name, &unique, &origin, &partial)

		cols := []string{}
		colInfo, err := db.Query(
			fmt.Sprintf("PRAGMA index_info(%q)", name),
		)
		if err != nil {
			return err
		}

		for colInfo.Next() {
			var seqno, cid int
			var cname string
			colInfo.Scan(&seqno, &cid, &cname)
			cols = append(cols, cname)
		}
		colInfo.Close()

		desc := ""
		if origin == "pk" {
			desc += "PRIMARY KEY"
		} else if origin == "u" {
			desc += "UNIQUE CONSTRAINT"
		}
		desc += fmt.Sprintf(" (btree: %s)", strings.Join(cols, ", "))
		idxTable.AppendRow(table.Row{name, desc})
	}
	if idxTable.Length() > 0 {
		fmt.Println("\n🔖 Indexes")
		idxTable.Render()
	}

	// Foreign keys
	fkRows, err := db.Query(
		fmt.Sprintf("PRAGMA foreign_key_list(%q)", tableName),
	)
	defer fkRows.Close()

	if err != nil {
		return err
	}

	fkTable := table.NewWriter()
	fkTable.SetOutputMirror(os.Stdout)
	fkTable.SetStyle(psqlStyle)
	fkTable.AppendHeader(table.Row{"From", "To Table", "To Column"})

	for fkRows.Next() {
		var id, seq int
		var refTable, from, to, onUpdate, onDelete, match string
		fkRows.Scan(
			&id, &seq, &refTable, &from, &to, &onUpdate,
			&onDelete, &match,
		)
		fkTable.AppendRow(table.Row{from, refTable, to})
	}
	if fkTable.Length() > 0 {
		fmt.Println("\n🔗 Foreign Keys")
		fkTable.Render()
	}

	fmt.Println()
	return nil
}

func getTableSuggestions() []prompt.Suggest {
	rows, err := db.Query(`SELECT name FROM sqlite_master
		             WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil
	}

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

func formatTimePadded(t time.Time) string {
	// Format the full second.
	base := t.Format("2006-01-02 15:04:05")

	// Extract microseconds (rounded).
	usec := t.Nanosecond() / 1000
	return fmt.Sprintf("%s.%06d", base, usec)
}

func formatValue(val interface{}) string {
	switch v := val.(type) {
	case nil:
		return "NULL"

	case []byte:
		return `\x` + strings.ToUpper(hex.EncodeToString(v))

	case time.Time:
		return formatTimePadded(v)

	default:
		return fmt.Sprintf("%v", v)
	}
}

func isNumeric(s string) bool {
	_, err := fmt.Sscanf(s, "%f", new(float64))
	return err == nil
}

func printPrettyTable(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		fmt.Printf("Failed to get columns: %v\n", err)
		return err
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(psqlStyle)
	t.Style().Format.Header = text.FormatLower
	t.AppendHeader(toRow(cols))

	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}

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

	return nil
}

func toRow(cols []string) table.Row {
	row := make(table.Row, len(cols))
	for i, col := range cols {
		row[i] = col
	}
	return row
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

func printJSON(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}

	var allRows []map[string]interface{}

	for rows.Next() {
		if err := rows.Scan(valPtrs...); err != nil {
			return err
		}

		row := make(map[string]interface{})
		for i, col := range cols {
			raw := *(valPtrs[i].(*interface{}))
			switch v := raw.(type) {
			case []byte:
				// Try to convert to string if printable,
				// otherwise hex.
				str := string(v)
				if isPrintable(str) {
					row[col] = str
				} else {
					row[col] = fmt.Sprintf(
						"\\x%s", hex.EncodeToString(v),
					)
				}
			default:
				row[col] = raw
			}
		}
		allRows = append(allRows, row)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(allRows)
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
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
