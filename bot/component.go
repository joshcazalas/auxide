package bot

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bwmarrin/discordgo"
)

func Component(s *discordgo.Session, i *discordgo.InteractionCreate) {
	if i.Type != discordgo.InteractionMessageComponent {
		return
	}
	userID := GetUserID(i)
	HandlePlayComponent(s, i, userID)
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
