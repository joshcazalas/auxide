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
	queues          map[string][]model.VideoInfo
	requestedBy     map[string]map[string]struct{}
	downloadedFiles map[string]string
}

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
		filepath, err := DownloadAudio(v.WebURL, v.Title)
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
	return append([]model.VideoInfo(nil), q.queues[channelID]...) // returns a copy
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

func GetQueue(discord *discordgo.Session, i *discordgo.InteractionCreate) {
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
