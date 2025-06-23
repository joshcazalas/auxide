package bot

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/bwmarrin/discordgo"
	"github.com/joshcazalas/discord-music-bot/model"
)

type Queue struct {
	sync.Mutex
	queues           map[string][]model.VideoInfo
	requestedBy      map[string]map[string]struct{}
	downloadedFiles  map[string]string
	inVoiceChannel   map[string]bool
	playing          map[string]bool
	voiceConnections map[string]*discordgo.VoiceConnection
}

func NewQueue() *Queue {
	return &Queue{
		queues:           make(map[string][]model.VideoInfo),
		requestedBy:      make(map[string]map[string]struct{}),
		downloadedFiles:  make(map[string]string),
		inVoiceChannel:   make(map[string]bool),
		playing:          make(map[string]bool),
		voiceConnections: make(map[string]*discordgo.VoiceConnection),
	}
}

var GlobalQueue = NewQueue()

func (q *Queue) Add(discord *discordgo.Session, guildID string, channelID string, userID string, video model.VideoInfo) {
	video.RequestedBy = userID

	q.Lock()
	q.queues[channelID] = append(q.queues[channelID], video)
	if q.requestedBy[channelID] == nil {
		q.requestedBy[channelID] = make(map[string]struct{})
	}
	q.requestedBy[channelID][userID] = struct{}{}
	q.Unlock()

	go func(v model.VideoInfo) {
		filepath, err := YoutubeDownloadAudio(v.WebURL, v.Title)
		if err != nil {
			log.Printf("Failed to download audio for %s: %v", v.Title, err)
			return
		}

		q.Lock()
		q.downloadedFiles[v.Title] = filepath
		q.Unlock()

		StartPlaybackIfNotActive(discord, guildID, channelID, filepath, userID)
	}(video)
}

func (q *Queue) Get(channelID string) []model.VideoInfo {
	q.Lock()
	defer q.Unlock()
	return append([]model.VideoInfo(nil), q.queues[channelID]...)
}

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

func (q *Queue) Clear(channelID string) {
	q.Lock()
	defer q.Unlock()
	delete(q.queues, channelID)
}

func (q *Queue) GetDownloadedFile(videoTitle string) (string, bool) {
	q.Lock()
	defer q.Unlock()
	path, ok := q.downloadedFiles[videoTitle]
	return path, ok
}

func (q *Queue) IsInVoiceChannel(guildID string) bool {
	q.Lock()
	defer q.Unlock()
	return q.inVoiceChannel[guildID]
}

func (q *Queue) SetInVoiceChannel(guildID string, in bool) {
	q.Lock()
	defer q.Unlock()
	q.inVoiceChannel[guildID] = in
}

func (q *Queue) IsPlaying(guildID string) bool {
	q.Lock()
	defer q.Unlock()
	return q.playing[guildID]
}

func (q *Queue) SetPlaying(guildID string, p bool) {
	q.Lock()
	defer q.Unlock()
	q.playing[guildID] = p
}

func (q *Queue) SaveVoiceConnection(guildID string, vc *discordgo.VoiceConnection) {
	q.Lock()
	defer q.Unlock()
	q.voiceConnections[guildID] = vc
}

func (q *Queue) GetVoiceConnection(guildID string) (*discordgo.VoiceConnection, bool) {
	q.Lock()
	defer q.Unlock()
	vc, ok := q.voiceConnections[guildID]
	return vc, ok
}

func (q *Queue) Peek(channelID string) (model.VideoInfo, bool) {
	q.Lock()
	defer q.Unlock()
	videos := q.queues[channelID]
	if len(videos) == 0 {
		return model.VideoInfo{}, false
	}
	return videos[0], true
}

func HandleGetQueueCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	channelID := i.ChannelID
	queue := GlobalQueue.Get(channelID)

	if len(queue) == 0 {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "The queue is currently empty.",
			},
		})
		return
	}

	var builder strings.Builder
	builder.WriteString("🎵 **Current Queue:**\n\n")
	for idx, video := range queue {
		builder.WriteString(fmt.Sprintf("**%d.** %s\n", idx+1, video.Title))
	}

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: builder.String(),
		},
	})
}

func HandleClearQueueCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	channelID := i.ChannelID
	GlobalQueue.Clear(channelID)
	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: "🗑️ The queue has been cleared.",
		},
	})
}
