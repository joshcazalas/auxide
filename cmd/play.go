package cmd

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/bwmarrin/discordgo"
	"github.com/joshcazalas/discord-music-bot/youtube"
)

var (
	searchResultsByUser = make(map[string][]youtube.VideoInfo)
	mu                  sync.Mutex
)

func Play(discord *discordgo.Session, i *discordgo.InteractionCreate, userID string) {
	query := i.ApplicationCommandData().Options[0].StringValue()

	err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
	})
	if err != nil {
		log.Println("Failed to defer interaction:", err)
		return
	}

	go func() {
		searchResults := youtube.Search(query)

		mu.Lock()
		searchResultsByUser[userID] = searchResults.Videos
		mu.Unlock()

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
		builder.WriteString("Please select a result from the list below:\n\n")
		for i, v := range searchResults.Videos {
			mins := int(v.Duration) / 60
			secs := int(v.Duration) % 60
			fmt.Fprintf(&builder, "**%d.** %s (%02d:%02d)\n", i+1, v.Title, mins, secs)
		}

		_, err := discord.FollowupMessageCreate(i.Interaction, true, &discordgo.WebhookParams{
			Content:    builder.String(),
			Components: components,
		})
		if err != nil {
			log.Println("Failed to send followup message:", err)
		}
	}()
}

func HandlePlayComponent(discord *discordgo.Session, i *discordgo.InteractionCreate, userID string) {
	if i.Type != discordgo.InteractionMessageComponent {
		return
	}

	customID := i.MessageComponentData().CustomID
	if !strings.HasPrefix(customID, "select_video_") {
		return
	}

	indexStr := strings.TrimPrefix(customID, "select_video_")
	index, _ := strconv.Atoi(indexStr)

	mu.Lock()
	videos, ok := searchResultsByUser[userID]
	mu.Unlock()

	if !ok || index <= 0 || index > len(videos) {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "Invalid selection. Please try again.",
			},
		})
		return
	}

	selected := videos[index-1]
	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: fmt.Sprintf("You selected **%s**\n%s", selected.Title, selected.WebURL),
		},
	})

	mu.Lock()
	delete(searchResultsByUser, userID)
	mu.Unlock()
}
