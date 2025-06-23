package bot

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/bwmarrin/discordgo"
)

func HandlePlayCommand(discord *discordgo.Session, i *discordgo.InteractionCreate, userID string) {
	query := i.ApplicationCommandData().Options[0].StringValue()

	err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
	})
	if err != nil {
		log.Println("Failed to defer interaction:", err)
		return
	}

	go func() {
		searchResults := Search(query)

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
				Content: "Invalid selection. Please try again.",
			},
		})
		return
	}

	selected := videos[index-1]
	GlobalQueue.Add(discord, i.GuildID, i.ChannelID, userID, selected)

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: fmt.Sprintf(
				"🎶 **Added to queue:**\n\n**%s**\n%s\n\n_Type `/queue` to view the current queue._",
				selected.Title,
				selected.WebURL,
			),
		},
	})

	DeleteSearchResults(userID)
}
