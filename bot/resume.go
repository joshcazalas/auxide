package bot

import (
	"fmt"

	"github.com/bwmarrin/discordgo"
)

func HandleResumeCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	guildID := i.GuildID
	channelID := i.ChannelID

	GlobalQueue.Lock()
	paused := GlobalQueue.paused[guildID]
	track, hasTrack := GlobalQueue.pausedTrack[guildID]
	GlobalQueue.Unlock()

	if !paused || !hasTrack {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "▶️ Nothing to resume.",
			},
		})
		return
	}

	GlobalQueue.Lock()
	GlobalQueue.paused[guildID] = false
	delete(GlobalQueue.pausedTrack, guildID)
	GlobalQueue.Unlock()

	GlobalQueue.Lock()
	GlobalQueue.queues[channelID] = append([]VideoInfo{track}, GlobalQueue.queues[channelID]...)
	GlobalQueue.Unlock()

	go StartPlaybackIfNotActive(discord, guildID, channelID)

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: fmt.Sprintf("▶️ Resuming: **%s**", track.Title),
		},
	})
}
