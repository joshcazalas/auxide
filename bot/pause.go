package bot

import (
	"log"

	"github.com/bwmarrin/discordgo"
)

func HandlePauseCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	guildID := i.GuildID
	channelID := i.ChannelID

	if !GlobalQueue.IsInVoiceChannel(guildID) || !GlobalQueue.IsPlaying(guildID) {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "⏸️ Nothing is playing to pause.",
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
			log.Printf("Pause signal sent for guild %s", guildID)
		default:
			log.Printf("Pause channel full or not listening for guild %s", guildID)
		}
	} else {
		log.Printf("No active stop channel for guild %s", guildID)
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "⚠️ Unable to pause playback.",
			},
		})
		return
	}

	current, ok := GlobalQueue.Peek(channelID)
	if !ok {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "⚠️ No current track found to pause.",
			},
		})
		return
	}

	GlobalQueue.Lock()
	GlobalQueue.paused[guildID] = true
	GlobalQueue.pausedTrack[guildID] = current
	GlobalQueue.SetPlaying(guildID, false)
	GlobalQueue.Unlock()

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: "⏸️ Playback paused.",
		},
	})
}
