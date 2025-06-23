package bot

import (
	"log"

	"github.com/bwmarrin/discordgo"
)

func HandleStopCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	guildID := i.GuildID
	channelID := i.ChannelID

	if !GlobalQueue.IsInVoiceChannel(guildID) {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "🔇 I'm not in a voice channel right now.",
			},
		})
		return
	}

	if !GlobalQueue.IsPlaying(guildID) {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "⏹️ There's nothing currently playing to stop.",
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

	GlobalQueue.SetPlaying(guildID, false)
	GlobalQueue.SetInVoiceChannel(guildID, false)

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: "⏹️ Playback stopped and queue cleared.",
		},
	})
}
