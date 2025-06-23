package bot

import (
	"math/rand"
	"time"

	"github.com/bwmarrin/discordgo"
)

func HandleShuffleCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	channelID := i.ChannelID

	GlobalQueue.Lock()
	queue := GlobalQueue.queues[channelID]
	if len(queue) <= 1 {
		GlobalQueue.Unlock()

		embed := &discordgo.MessageEmbed{
			Title:       "🔀 Not Enough Songs",
			Description: "There must be at least two songs in the queue to shuffle.",
			Color:       0x1DB954,
		}

		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Embeds: []*discordgo.MessageEmbed{embed},
			},
		})
		return
	}

	current := queue[0]
	rest := queue[1:]

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rng.Shuffle(len(rest), func(i, j int) {
		rest[i], rest[j] = rest[j], rest[i]
	})

	GlobalQueue.queues[channelID] = append([]VideoInfo{current}, rest...)
	GlobalQueue.Unlock()

	embed := &discordgo.MessageEmbed{
		Title:       "🔀 Queue Shuffled",
		Description: "The queue has been shuffled. The currently playing song was not affected.",
		Color:       0x1DB954,
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Use /queue to view the updated order.",
		},
	}

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})
}
