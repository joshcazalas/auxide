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
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "🔀 Not enough songs in the queue to shuffle.",
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

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: "🔀 Queue shuffled",
		},
	})
}
