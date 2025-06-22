package bot

import (
	"sync"

	model "github.com/joshcazalas/discord-music-bot/model"
)

var (
	mu                  sync.Mutex
	searchResultsByUser = make(map[string][]model.VideoInfo)
)

// GetSearchResults returns the videos for a user safely.
func GetSearchResults(userID string) ([]model.VideoInfo, bool) {
	mu.Lock()
	defer mu.Unlock()
	videos, ok := searchResultsByUser[userID]
	return videos, ok
}

// SetSearchResults stores videos for a user safely.
func SetSearchResults(userID string, videos []model.VideoInfo) {
	mu.Lock()
	defer mu.Unlock()
	searchResultsByUser[userID] = videos
}

// DeleteSearchResults removes stored videos for a user safely.
func DeleteSearchResults(userID string) {
	mu.Lock()
	defer mu.Unlock()
	delete(searchResultsByUser, userID)
}
