package bot

import (
	"context"
	"os/exec"
	"path/filepath"
	"regexp"
	"time"
)

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[^\w\-.]`)
	return re.ReplaceAllString(name, "_")
}

func DownloadAudio(url string, title string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	safeTitle := sanitizeFilename(title)
	outputFile := filepath.Join("/tmp", safeTitle+".mp3")

	args := []string{
		"-x",
		"--audio-format", "mp3",
		"-o", outputFile,
		url,
	}

	cmd := exec.CommandContext(ctx, "yt-dlp", args...)
	_, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}

	return outputFile, nil
}
