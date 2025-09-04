package input

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type LokiReader struct {
	Query  string
	Url    string
	client *http.Client
	buffer *bytes.Buffer
	eof    bool
}

func (lr *LokiReader) Read(p []byte) (n int, err error) {
	if lr.buffer == nil {
		lr.buffer = &bytes.Buffer{}
	}
	if lr.client == nil {
		lr.client = &http.Client{Timeout: 10 * time.Second}
	}

	// If buffer is empty and not EOF, fetch more data
	for lr.buffer.Len() == 0 && !lr.eof {
		// If fetch() sets eof and buffer is still empty, break
		if lr.eof && lr.buffer.Len() == 0 {
			return 0, io.EOF
		}
	}

	n, err = lr.buffer.Read(p)
	if n == 0 && lr.eof {
		return 0, io.EOF
	}
	return n, err
}

// fetch gets data from Loki Tail API and fills the buffer
func (lr *LokiReader) fetch() {
	// Build Loki Tail API WebSocket URL
	wsUrl := fmt.Sprintf("wss://%s/loki/api/v1/tail?query=%s&limit=1000", strings.TrimPrefix(lr.Url, "https://"), url.QueryEscape(lr.Query))

	// Connect to WebSocket
	conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	if err != nil {
		log.Println("XXXXXXXXXXXXXXXXXXXXWebSocket dial error:", err)
		return
	}
	defer conn.Close()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("XXXXXXXXXXXXXXXXXXXXXWebSocket read error:", err)

			if websocket.IsCloseError(err, websocket.CloseNormalClosure) || err == io.EOF {
				lr.eof = true
				break
			}
			return
		}

		var result struct {
			Streams []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"streams"`
			DroppedEntries interface{} `json:"dropped_entries"`
		}
		if err := json.Unmarshal(message, &result); err != nil {
			log.Println("JSON unmarshal error:", err)
			continue
			// return
		}

		for _, stream := range result.Streams {
			for _, val := range stream.Values {
				if len(val) > 1 {
					// Parse JSON
					var jsonData map[string]interface{}
					if err := json.Unmarshal([]byte(val[1]), &jsonData); err != nil {
						log.Println("JSON unmarshal error:", err)
						continue
					}
					// Transform jsonData[log] into string
					if log, ok := jsonData["log"].(string); ok {
						lr.buffer.WriteString(log + "\n")
					}
				}
			}
		}

	}

	if lr.buffer.Len() == 0 {
		lr.eof = true
	}

}

func CreateLokiStream(lokiUrl string) (io.Reader, error) {
	// Create an io.Reader that implements a Loki stream
	// Parse lokiUrl
	parsedURL, err := url.Parse(lokiUrl)
	if err != nil {
		return nil, err
	}

	// Get query param from url
	query := parsedURL.Query().Get("query")
	finalUrl := fmt.Sprintf("https://%s", parsedURL.Host)
	lokiReader := &LokiReader{Query: query, Url: finalUrl}
	go lokiReader.fetch() // Initial fetch to populate buffer
	return lokiReader, nil
}
