package bot

import (
	"sync"

	"github.com/joshcazalas/discord-music-bot/model"
)

type Queue struct {
	sync.Mutex
	queues map[string][]model.VideoInfo
}

var GlobalQueue = &Queue{
	queues: make(map[string][]model.VideoInfo),
}

// Add song to queue
func (q *Queue) Add(channelID string, video model.VideoInfo) {
	q.Lock()
	defer q.Unlock()
	q.queues[channelID] = append(q.queues[channelID], video)
}

// Get queue
func (q *Queue) Get(channelID string) []model.VideoInfo {
	q.Lock()
	defer q.Unlock()
	return append([]model.VideoInfo(nil), q.queues[channelID]...) // returns a copy
}

// Removes and returns the first song in the queue
func (q *Queue) Pop(channelID string) (model.VideoInfo, bool) {
	q.Lock()
	defer q.Unlock()
	videos := q.queues[channelID]
	if len(videos) == 0 {
		return model.VideoInfo{}, false
	}
	video := videos[0]
	q.queues[channelID] = videos[1:]
	return video, true
}

// Clear queue
func (q *Queue) Clear(channelID string) {
	q.Lock()
	defer q.Unlock()
	delete(q.queues, channelID)
}
