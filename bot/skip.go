package bot

import (
	"fmt"
	"log"

	"github.com/bwmarrin/discordgo"
)

func HandleSkipCommand(discord *discordgo.Session, i *discordgo.InteractionCreate) {
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
				Content: "⏹️ There's nothing currently playing to skip.",
			},
		})
		return
	}

	next, ok := GlobalQueue.Peek(channelID)
	if !ok {
		discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "📭 No more songs in the queue to skip to.",
			},
		})
		return
	}

	GlobalQueue.Lock()
	stopChan, found := GlobalQueue.stopChans[guildID]
	GlobalQueue.Unlock()

	_, popped := GlobalQueue.Pop(channelID)
	if !popped {
		log.Printf("Warning: tried to pop current track in skip but queue was empty for channel %s", channelID)
	}

	if found {
		select {
		case stopChan <- true:
			log.Printf("Skip signal sent for guild %s", guildID)
		default:
			log.Printf("Skip channel full or not listening for guild %s", guildID)
		}
	} else {
		log.Printf("No active stop channel for guild %s", guildID)
	}

	GlobalQueue.SetPlaying(guildID, false)

	discord.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: fmt.Sprintf("⏭️ Skipping to next track: **%s**", next.Title),
		},
	})

	go StartPlaybackIfNotActive(discord, guildID, channelID)
}
