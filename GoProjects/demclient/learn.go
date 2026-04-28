package demclient

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
)

// StreamData streams an SML DataFrame to a project.
func (c *Client) StreamData(pid string, df *SMLDataFrame) error {
	body, err := json.Marshal(df)
	if err != nil {
		return fmt.Errorf("marshal dataframe: %w", err)
	}

	reqURL := c.BaseURL() + "/projects/" + url.PathEscape(pid) + "/learn"
	resp, err := c.DoRequest("POST", reqURL, string(body))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return fmt.Errorf("stream data failed (%d): %s", resp.StatusCode, resp.StatusString)
	}
	return nil
}

// StreamDataJSON streams raw SML DataFrame JSON string to a project.
func (c *Client) StreamDataJSON(pid string, smlJSON string) error {
	reqURL := c.BaseURL() + "/projects/" + url.PathEscape(pid) + "/learn"
	resp, err := c.DoRequest("POST", reqURL, smlJSON)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return fmt.Errorf("stream data failed (%d): %s", resp.StatusCode, resp.StatusString)
	}
	return nil
}

// StreamCSV reads a CSV from reader, converts to SML DataFrame chunks, and streams
// each chunk to the project. chunkSize controls rows per API call (0 = all at once).
func (c *Client) StreamCSV(pid string, reader io.Reader, chunkSize int) (int, error) {
	csvReader := csv.NewReader(reader)

	// Read header
	header, err := csvReader.Read()
	if err != nil {
		return 0, fmt.Errorf("read CSV header: %w", err)
	}

	// Read all rows
	var allRows [][]string
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return len(allRows), fmt.Errorf("read CSV row %d: %w", len(allRows)+1, err)
		}
		allRows = append(allRows, row)
	}

	if len(allRows) == 0 {
		return 0, nil
	}

	// Auto-detect column types from first row
	types := detectColumnTypes(header, allRows)

	// Stream in chunks
	if chunkSize <= 0 {
		chunkSize = len(allRows)
	}

	totalStreamed := 0
	for start := 0; start < len(allRows); start += chunkSize {
		end := start + chunkSize
		if end > len(allRows) {
			end = len(allRows)
		}

		chunk := allRows[start:end]
		df := csvRowsToSML(header, types, chunk)

		if err := c.StreamData(pid, df); err != nil {
			return totalStreamed, fmt.Errorf("stream chunk at row %d: %w", start, err)
		}
		totalStreamed += len(chunk)
	}

	return totalStreamed, nil
}

// detectColumnTypes guesses SML types from CSV data.
// Returns "C" for numeric, "S" for string, "B" for binary (0/1 only).
func detectColumnTypes(header []string, rows [][]string) []string {
	types := make([]string, len(header))

	for col := range header {
		allNumeric := true
		allBinary := true
		for _, row := range rows {
			if col >= len(row) {
				continue
			}
			val := strings.TrimSpace(row[col])
			if val == "" || val == "NaN" || val == "nan" {
				continue
			}

			_, err := strconv.ParseFloat(val, 64)
			if err != nil {
				allNumeric = false
				allBinary = false
				break
			}

			if val != "0" && val != "1" && val != "0.0" && val != "1.0" {
				allBinary = false
			}
		}

		if allBinary && allNumeric {
			types[col] = "B"
		} else if allNumeric {
			types[col] = "C"
		} else {
			types[col] = "S"
		}
	}

	return types
}

// csvRowsToSML converts CSV rows to an SML DataFrame.
func csvRowsToSML(header, types []string, rows [][]string) *SMLDataFrame {
	data := make([][]any, len(rows))

	for i, row := range rows {
		dataRow := make([]any, len(header))
		for j := range header {
			if j >= len(row) {
				dataRow[j] = nil
				continue
			}
			val := strings.TrimSpace(row[j])

			switch types[j] {
			case "C", "B":
				if val == "" || val == "NaN" || val == "nan" {
					dataRow[j] = math.NaN()
				} else {
					f, err := strconv.ParseFloat(val, 64)
					if err != nil {
						dataRow[j] = math.NaN()
					} else {
						dataRow[j] = f
					}
				}
			default:
				dataRow[j] = val
			}
		}
		data[i] = dataRow
	}

	return &SMLDataFrame{
		AttributeNames: header,
		AttributeTypes: types,
		Data:           data,
	}
}
