package bot

import (
	"log"

	"github.com/bwmarrin/discordgo"
)

func HandleStopCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	guildID := i.GuildID
	channelID := i.ChannelID

	if !GlobalQueue.IsInVoiceChannel(guildID) {
		embed := &discordgo.MessageEmbed{
			Title:       "🔇 Not in Voice Channel",
			Description: "I'm not currently in a voice channel.",
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

	if !GlobalQueue.IsPlaying(guildID) {
		embed := &discordgo.MessageEmbed{
			Title:       "⏹️ Nothing Playing",
			Description: "There's no track currently playing to stop.",
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
	stopChan, found := GlobalQueue.stopChans[guildID]
	GlobalQueue.Unlock()

	if found {
		select {
		case stopChan <- true:
			log.Printf("Stop signal sent for guild %s", guildID)
		default:
			log.Printf("Stop channel full or not listening for guild %s", guildID)
		}
	} else {
		log.Printf("No active stop channel for guild %s", guildID)
	}

	GlobalQueue.Clear(channelID)

	vc, ok := GlobalQueue.GetVoiceConnection(guildID)
	if ok && vc != nil {
		if err := vc.Disconnect(); err != nil {
			log.Printf("Failed to disconnect voice connection for guild %s: %v", guildID, err)
		} else {
			log.Printf("Disconnected voice connection for guild %s", guildID)
		}
	}

	CancelIdleMonitor(guildID)

	GlobalQueue.SetPlaying(guildID, false)
	GlobalQueue.SetInVoiceChannel(guildID, false)

	embed := &discordgo.MessageEmbed{
		Title:       "⏹️ Playback Stopped",
		Description: "Playback has been stopped, the queue has been cleared, and I've left the voice channel.",
		Color:       0x1DB954,
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Try /play to add new songs, or /help for all commands",
		},
	}

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	})
}
