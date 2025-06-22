package bot

import (
	"fmt"
	"log"
	"strings"

	"github.com/bwmarrin/discordgo"
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
