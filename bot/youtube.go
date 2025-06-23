package bot

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type VideoInfo struct {
	ID          string  `json:"id"`
	Title       string  `json:"title"`
	Uploader    string  `json:"uploader"`
	WebURL      string  `json:"webpage_url"`
	Duration    float64 `json:"duration"`
	RequestedBy string
}

type SearchResult struct {
	Message string
	Videos  []VideoInfo
}

var youtubeRegex = regexp.MustCompile(`^(https?://)?(www\.)?(youtube\.com|youtu\.be)/.+$`)

func isYouTubeLink(input string) bool {
	return youtubeRegex.MatchString(input)
}

func sanitizeYouTubeURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := parsed.Query()
	videoID := q.Get("v")
	if videoID == "" {
		return raw
	}
	return "https://www.youtube.com/watch?v=" + videoID
}

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[^\w\-.]`)
	return re.ReplaceAllString(name, "_")
}

func YoutubeSearch(query string) SearchResult {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	args := []string{
		"--dump-json",
		"--no-download",
		"--flat-playlist",
		"--default-search", "ytsearch5",
		query,
	}

	cmd := exec.CommandContext(ctx, "yt-dlp", args...)
	cmd.Env = append(cmd.Env, "PYTHONIOENCODING=utf-8")

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("failed to get stdout pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		log.Fatalf("failed to start yt-dlp: %v", err)
	}

	scanner := bufio.NewScanner(stdoutPipe)
	videos, err := parseYTDLPJSONLines(scanner)
	if err != nil {
		log.Fatalf("error reading yt-dlp output: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		log.Fatalf("yt-dlp command failed: %v", err)
	}

	var builder strings.Builder
	for i, video := range videos {
		minutes := int(video.Duration) / 60
		seconds := int(video.Duration) % 60
		fmt.Fprintf(&builder, "Result #%d:\n", i+1)
		fmt.Fprintf(&builder, "Title: %s\n", video.Title)
		fmt.Fprintf(&builder, "Channel: %s\n", video.Uploader)
		fmt.Fprintf(&builder, "URL: %s\n", video.WebURL)
		fmt.Fprintf(&builder, "Duration: %02d:%02d\n\n", minutes, seconds)
	}

	return SearchResult{
		Message: builder.String(),
		Videos:  videos,
	}
}

func YoutubeDownloadAudio(url string, title string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	safeTitle := sanitizeFilename(title)
	AudioPath := filepath.Join("/tmp", safeTitle+".mp3")

	cmd := exec.CommandContext(ctx, "yt-dlp", "-f", "bestaudio", "-x", "--audio-format", "mp3", "-o", AudioPath, url)
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("yt-dlp download failed: %w, output: %s", err, string(output))
	}

	return AudioPath, nil
}

func YoutubeGetInfo(url string) (VideoInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "yt-dlp", "--dump-json", "--no-playlist", url)
	cmd.Env = append(cmd.Env, "PYTHONIOENCODING=utf-8")

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return VideoInfo{}, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	_, err = cmd.StderrPipe()
	if err != nil {
		return VideoInfo{}, fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return VideoInfo{}, fmt.Errorf("failed to start yt-dlp: %w", err)
	}

	decoder := json.NewDecoder(stdoutPipe)

	var videos []VideoInfo
	for {
		var video VideoInfo
		if err := decoder.Decode(&video); err != nil {
			if err == io.EOF {
				break
			}
			return VideoInfo{}, fmt.Errorf("error decoding JSON from yt-dlp: %w", err)
		}
		videos = append(videos, video)
	}

	if err := cmd.Wait(); err != nil {
		return VideoInfo{}, fmt.Errorf("yt-dlp command failed: %w", err)
	}

	if len(videos) == 0 {
		return VideoInfo{}, fmt.Errorf("no video info returned for URL")
	}

	log.Printf("yt-dlp parsed video info: %+v", videos[0])

	return videos[0], nil
}

func parseYTDLPJSONLines(scanner *bufio.Scanner) ([]VideoInfo, error) {
	var videos []VideoInfo
	for scanner.Scan() {
		line := scanner.Text()
		var info VideoInfo
		if err := json.Unmarshal([]byte(line), &info); err != nil {
			log.Printf("Skipping invalid JSON line: %v", err)
			continue
		}
		videos = append(videos, info)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return videos, nil
}
