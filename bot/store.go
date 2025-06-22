package bot

import (
	"sync"

	"github.com/bwmarrin/discordgo"
	model "github.com/joshcazalas/discord-music-bot/model"
)

var GlobalQueue = &Queue{
	queues:          make(map[string][]model.VideoInfo),
	requestedBy:     make(map[string]map[string]struct{}),
	downloadedFiles: make(map[string]string),
}

var (
	mu                  sync.Mutex
	searchResultsByUser = make(map[string][]model.VideoInfo)
)

var (
	inVoiceChannelMu sync.Mutex
	inVoiceChannel   = make(map[string]bool)
)

var (
	playingMu sync.Mutex
	playing   = make(map[string]bool)
)

var (
	voiceConnectionsMu sync.Mutex
	voiceConnections   = make(map[string]*discordgo.VoiceConnection) // guildID -> VoiceConnection
)

func GetSearchResults(userID string) ([]model.VideoInfo, bool) {
	mu.Lock()
	defer mu.Unlock()
	videos, ok := searchResultsByUser[userID]
	return videos, ok
}

func SetSearchResults(userID string, videos []model.VideoInfo) {
	mu.Lock()
	defer mu.Unlock()
	searchResultsByUser[userID] = videos
}

func DeleteSearchResults(userID string) {
	mu.Lock()
	defer mu.Unlock()
	delete(searchResultsByUser, userID)
}

func IsInVoiceChannel(guildID string) bool {
	inVoiceChannelMu.Lock()
	defer inVoiceChannelMu.Unlock()
	return inVoiceChannel[guildID]
}

func SetInVoiceChannel(guildID string, in bool) {
	inVoiceChannelMu.Lock()
	defer inVoiceChannelMu.Unlock()
	inVoiceChannel[guildID] = in
}

func IsPlaying(guildID string) bool {
	playingMu.Lock()
	defer playingMu.Unlock()
	return playing[guildID]
}

func SetPlaying(guildID string, p bool) {
	playingMu.Lock()
	defer playingMu.Unlock()
	playing[guildID] = p
}

func SaveVoiceConnection(guildID string, vc *discordgo.VoiceConnection) {
	voiceConnectionsMu.Lock()
	defer voiceConnectionsMu.Unlock()
	voiceConnections[guildID] = vc
}

func GetVoiceConnection(guildID string) (*discordgo.VoiceConnection, bool) {
	voiceConnectionsMu.Lock()
	defer voiceConnectionsMu.Unlock()
	vc, ok := voiceConnections[guildID]
	return vc, ok
}
