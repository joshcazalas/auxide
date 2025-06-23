package bot

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/joshcazalas/discord-music-bot/model"
)

func YoutubeSearch(query string) model.SearchResult {
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

	var videos []model.VideoInfo
	scanner := bufio.NewScanner(stdoutPipe)
	for scanner.Scan() {
		line := scanner.Text()
		var info model.VideoInfo
		if err := json.Unmarshal([]byte(line), &info); err != nil {
			log.Printf("Skipping invalid JSON line: %v", err)
			continue
		}
		videos = append(videos, info)
	}

	if err := scanner.Err(); err != nil {
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

	return model.SearchResult{
		Message: builder.String(),
		Videos:  videos,
	}
}

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[^\w\-.]`)
	return re.ReplaceAllString(name, "_")
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
