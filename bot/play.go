package bot

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
)

var (
	mu                  sync.Mutex
	searchResultsByUser = make(map[string][]VideoInfo)
)

var youtubeRegex = regexp.MustCompile(`^(https?://)?(www\.)?(youtube\.com|youtu\.be)/.+$`)

func isYouTubeLink(input string) bool {
	return youtubeRegex.MatchString(input)
}

func GetSearchResults(userID string) ([]VideoInfo, bool) {
	mu.Lock()
	defer mu.Unlock()
	videos, ok := searchResultsByUser[userID]
	return videos, ok
}

func SetSearchResults(userID string, videos []VideoInfo) {
	mu.Lock()
	defer mu.Unlock()
	searchResultsByUser[userID] = videos
}

func DeleteSearchResults(userID string) {
	mu.Lock()
	defer mu.Unlock()
	delete(searchResultsByUser, userID)
}

func HandlePlayCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	userID := GetUserID(i)
	query := i.ApplicationCommandData().Options[0].StringValue()

	err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
	})
	if err != nil {
		log.Println("Failed to defer interaction:", err)
		return
	}

	go func() {
		if isYouTubeLink(query) {
			video, err := YoutubeGetInfo(query)
			if err != nil {
				log.Printf("Failed to fetch info for YouTube link %s: %v", query, err)
				discord.FollowupMessageCreate(i.Interaction, true, &discordgo.WebhookParams{
					Content: "❌ Failed to get video info. Please make sure the link is valid.",
				})
				return
			}

			GlobalQueue.Add(discord, i.GuildID, i.ChannelID, userID, video)

			duration := time.Duration(video.Duration) * time.Second
			embed := &discordgo.MessageEmbed{
				Title:       "✅ Added to Queue",
				Description: fmt.Sprintf("[%s](%s)", video.Title, video.WebURL),
				Color:       0x1DB954,
				Fields: []*discordgo.MessageEmbedField{
					{
						Name:   "Requested By",
						Value:  fmt.Sprintf("<@%s>", userID),
						Inline: true,
					},
					{
						Name:   "Duration",
						Value:  fmtDuration(duration),
						Inline: true,
					},
				},
				Footer: &discordgo.MessageEmbedFooter{
					Text: "Use /queue to view the current queue.",
				},
			}

			discord.FollowupMessageCreate(i.Interaction, true, &discordgo.WebhookParams{
				Embeds: []*discordgo.MessageEmbed{embed},
			})
			return
		}

		searchResults := YoutubeSearch(query)

		SetSearchResults(userID, searchResults.Videos)

		var buttons []discordgo.MessageComponent
		for i := range searchResults.Videos {
			buttons = append(buttons, discordgo.Button{
				Label:    fmt.Sprintf("%d", i+1),
				Style:    discordgo.PrimaryButton,
				CustomID: fmt.Sprintf("select_video_%d", i+1),
			})
		}

		components := []discordgo.MessageComponent{
			discordgo.ActionsRow{Components: buttons},
		}

		var builder strings.Builder
		for i, v := range searchResults.Videos {
			mins := int(v.Duration) / 60
			secs := int(v.Duration) % 60
			fmt.Fprintf(&builder, "**%d.** %s (%02d:%02d)\n", i+1, v.Title, mins, secs)
		}

		embed := &discordgo.MessageEmbed{
			Title:       "🔍 Search Results",
			Description: builder.String(),
			Color:       0x1DB954,
			Footer: &discordgo.MessageEmbedFooter{
				Text: "Click a number below to choose a song",
			},
		}

		_, err := discord.FollowupMessageCreate(i.Interaction, true, &discordgo.WebhookParams{
			Embeds:     []*discordgo.MessageEmbed{embed},
			Components: components,
		})
		if err != nil {
			log.Println("Failed to send followup message:", err)
		}
	}()
}

func HandlePlaySelection(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	if i.Type != discordgo.InteractionMessageComponent {
		return
	}
	userID := GetUserID(i)

	customID := i.MessageComponentData().CustomID
	if !strings.HasPrefix(customID, "select_video_") {
		return
	}

	indexStr := strings.TrimPrefix(customID, "select_video_")
	index, _ := strconv.Atoi(indexStr)

	videos, ok := GetSearchResults(userID)

	if !ok || index <= 0 || index > len(videos) {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "❌ Invalid selection. Please try again.",
			},
		})
		return
	}

	selected := videos[index-1]
	GlobalQueue.Add(discord, i.GuildID, i.ChannelID, userID, selected)

	duration := time.Duration(selected.Duration) * time.Second
	embed := &discordgo.MessageEmbed{
		Title:       "✅ Added to Queue",
		Description: fmt.Sprintf("[%s](%s)", selected.Title, selected.WebURL),
		Color:       0x1DB954,
		Fields: []*discordgo.MessageEmbedField{
			{
				Name:   "Requested By",
				Value:  fmt.Sprintf("<@%s>", userID),
				Inline: true,
			},
			{
				Name:   "Duration",
				Value:  fmtDuration(duration),
				Inline: true,
			},
		},
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Use /queue to view the current queue.",
		},
	}

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})

	DeleteSearchResults(userID)
}
