package model

type VideoInfo struct {
	ID       string  `json:"id"`
	Title    string  `json:"title"`
	Uploader string  `json:"uploader"`
	WebURL   string  `json:"webpage_url"`
	Duration float64 `json:"duration"`
}

type SearchResult struct {
	Message string
	Videos  []VideoInfo
}
