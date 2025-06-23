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
		embed := &discordgo.MessageEmbed{
			Title:       "▶️ Nothing to Resume",
			Description: "There is no paused track to resume.",
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

	GlobalQueue.Lock()
	GlobalQueue.paused[guildID] = false
	delete(GlobalQueue.pausedTrack, guildID)
	GlobalQueue.queues[channelID] = append([]VideoInfo{track}, GlobalQueue.queues[channelID]...)
	GlobalQueue.Unlock()

	go StartPlaybackIfNotActive(discord, guildID, channelID)

	embed := &discordgo.MessageEmbed{
		Title:       "▶️ Resuming Playback",
		Description: fmt.Sprintf("Resuming: **%s**", track.Title),
		Color:       0x1DB954,
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Use /pause if you want to stop playback again.",
		},
	}

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})
}
