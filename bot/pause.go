package bot

import (
	"log"

	"github.com/bwmarrin/discordgo"
)

func HandlePauseCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
	guildID := i.GuildID

	if !GlobalQueue.IsInVoiceChannel(guildID) || !GlobalQueue.IsPlaying(guildID) {
		embed := &discordgo.MessageEmbed{
			Title:       "⏸️ Nothing to Pause",
			Description: "There's nothing currently playing that can be paused.",
			Color:       0x1DB954,
		}
		if err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Embeds: []*discordgo.MessageEmbed{embed},
			},
		}); err != nil {
			log.Printf("Failed to send 'Nothing to Pause' response: %v", err)
		}
		return
	}

	current, ok := GlobalQueue.GetCurrentlyPlaying(guildID)
	if !ok {
		embed := &discordgo.MessageEmbed{
			Title:       "⚠️ No Current Track",
			Description: "Couldn't find a track to pause.",
			Color:       0x1DB954,
		}
		if err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Embeds: []*discordgo.MessageEmbed{embed},
			},
		}); err != nil {
			log.Printf("Failed to send 'No Current Track' response: %v", err)
		}
		return
	}

	GlobalQueue.Lock()
	stopChan, found := GlobalQueue.stopChans[guildID]
	GlobalQueue.Unlock()

	if !found {
		log.Printf("No active stop channel for guild %s", guildID)
		embed := &discordgo.MessageEmbed{
			Title:       "⚠️ Unable to Pause",
			Description: "Something went wrong trying to pause playback.",
			Color:       0x1DB954,
		}
		if err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Embeds: []*discordgo.MessageEmbed{embed},
			},
		}); err != nil {
			log.Printf("Failed to send 'Unable to Pause' response: %v", err)
		}
		return
	}

	embed := &discordgo.MessageEmbed{
		Title:       "⏸️ Playback Paused",
		Description: "The current track has been paused.\n\nUse `/resume` to continue playback.",
		Color:       0x1DB954,
		Footer: &discordgo.MessageEmbedFooter{
			Text: "Use /queue to see what's next.",
		},
	}

	if err := discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Embeds: []*discordgo.MessageEmbed{embed},
		},
	}); err != nil {
		log.Printf("Failed to send 'Playback Paused' response: %v", err)
	}

	select {
	case stopChan <- true:
		log.Printf("Pause signal sent for guild %s", guildID)
	default:
		log.Printf("Pause channel full or not listening for guild %s", guildID)
	}

	GlobalQueue.Lock()
	GlobalQueue.paused[guildID] = true
	GlobalQueue.pausedTrack[guildID] = current
	GlobalQueue.SetPlaying(guildID, false)
	GlobalQueue.Unlock()
}
