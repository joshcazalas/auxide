package bot

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
)

type Queue struct {
	sync.Mutex
	queues           map[string][]VideoInfo
	requestedBy      map[string]map[string]struct{}
	downloadedFiles  map[string]string
	inVoiceChannel   map[string]bool
	playing          map[string]bool
	voiceConnections map[string]*discordgo.VoiceConnection
	stopChans        map[string]chan bool
	paused           map[string]bool
	pausedTrack      map[string]VideoInfo
	lastActivity     map[string]time.Time
	idleCancelFuncs  map[string]context.CancelFunc
	shuffleMode      map[string]bool
}

func NewQueue() *Queue {
	return &Queue{
		queues:           make(map[string][]VideoInfo),
		requestedBy:      make(map[string]map[string]struct{}),
		downloadedFiles:  make(map[string]string),
		inVoiceChannel:   make(map[string]bool),
		playing:          make(map[string]bool),
		voiceConnections: make(map[string]*discordgo.VoiceConnection),
		stopChans:        make(map[string]chan bool),
		paused:           make(map[string]bool),
		pausedTrack:      make(map[string]VideoInfo),
		lastActivity:     make(map[string]time.Time),
		idleCancelFuncs:  make(map[string]context.CancelFunc),
		shuffleMode:      make(map[string]bool),
	}
}

var GlobalQueue = NewQueue()

func (q *Queue) Add(discord *discordgo.Session, guildID string, channelID string, userID string, video VideoInfo) {
	video.RequestedBy = userID

	q.Lock()
	q.queues[channelID] = append(q.queues[channelID], video)
	if q.requestedBy[channelID] == nil {
		q.requestedBy[channelID] = make(map[string]struct{})
	}
	q.requestedBy[channelID][userID] = struct{}{}
	q.Unlock()

	go func(v VideoInfo) {
		filepath, err := YoutubeDownloadAudio(v.WebURL, v.Title)
		if err != nil {
			log.Printf("Failed to download audio for %s: %v", v.Title, err)
			return
		}

		q.Lock()
		q.downloadedFiles[v.Title] = filepath
		q.Unlock()

		StartPlaybackIfNotActive(discord, guildID, channelID)
	}(video)
}

func (q *Queue) Get(channelID string) []VideoInfo {
	q.Lock()
	defer q.Unlock()
	return append([]VideoInfo(nil), q.queues[channelID]...)
}

func (q *Queue) Pop(channelID string) (VideoInfo, bool) {
	q.Lock()
	defer q.Unlock()
	videos := q.queues[channelID]
	if len(videos) == 0 {
		return VideoInfo{}, false
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

func (q *Queue) Peek(channelID string) (VideoInfo, bool) {
	q.Lock()
	defer q.Unlock()
	videos := q.queues[channelID]
	if len(videos) == 0 {
		return VideoInfo{}, false
	}
	return videos[0], true
}

func (q *Queue) SetLastActivity(guildID string) {
	q.Lock()
	defer q.Unlock()
	q.lastActivity[guildID] = time.Now()
}

func (q *Queue) GetLastActivity(guildID string) time.Time {
	q.Lock()
	defer q.Unlock()
	return q.lastActivity[guildID]
}

func (q *Queue) IsShuffleEnabled(channelID string) bool {
	q.Lock()
	defer q.Unlock()
	return q.shuffleMode[channelID]
}

func (q *Queue) SetShuffle(channelID string, enabled bool) {
	q.Lock()
	defer q.Unlock()
	q.shuffleMode[channelID] = enabled
}

func (q *Queue) PopRandom(channelID string) (VideoInfo, bool) {
	q.Lock()
	defer q.Unlock()

	queue := q.queues[channelID]
	if len(queue) == 0 {
		return VideoInfo{}, false
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	idx := rng.Intn(len(queue))
	selected := queue[idx]

	q.queues[channelID] = append(queue[:idx], queue[idx+1:]...)
	return selected, true
}

func (q *Queue) RemoveByTitle(channelID, title string) {
	q.Lock()
	defer q.Unlock()

	queue := q.queues[channelID]
	newQueue := make([]VideoInfo, 0, len(queue))
	for _, item := range queue {
		if item.Title != title {
			newQueue = append(newQueue, item)
		}
	}
	q.queues[channelID] = newQueue
}

func HandleGetQueueCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	channelID := i.ChannelID
	queue := GlobalQueue.Get(channelID)

	if len(queue) == 0 {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Embeds: []*discordgo.MessageEmbed{
					{
						Title:       "🎵 Current Queue",
						Description: "The queue is currently empty.",
						Color:       0x1DB954,
						Footer: &discordgo.MessageEmbedFooter{
							Text: "Try /shuffle, /skip, /pause & more. Use /help to see all commands",
						},
					},
				},
			},
		})
		return
	}

	var builder strings.Builder
	for idx, video := range queue {
		builder.WriteString(fmt.Sprintf(
			"**%d.** [%s](%s)\nRequested By: <@%s>\n\n",
			idx+1, video.Title, video.WebURL, video.RequestedBy,
		))
	}

	embed := &discordgo.MessageEmbed{
		Title:       "🎵 Current Queue",
		Description: builder.String(),
		Color:       0x1DB954,
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Try /shuffle, /skip, /pause & more. Use /help to see all commands",
		},
	}

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})
}

func HandleClearQueueCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	channelID := i.ChannelID
	GlobalQueue.Clear(channelID)

	embed := &discordgo.MessageEmbed{
		Title:       "🗑️ Queue Cleared",
		Description: "The queue has been successfully cleared.",
		Color:       0x1DB954,
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Try /play to add new songs, or /help for all commands",
		},
	}

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})
}
