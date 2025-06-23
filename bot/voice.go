package bot

import (
	"errors"
	"fmt"
	"log"

	"github.com/bwmarrin/dgvoice"
	"github.com/bwmarrin/discordgo"
)

func findUserVoiceChannel(discord *discordgo.Session, guildID, userID string) string {
	guild, err := discord.State.Guild(guildID)
	if err != nil {
		log.Printf("Failed to fetch guild state: %v", err)
		return ""
	}

	for _, vs := range guild.VoiceStates {
		if vs.UserID == userID {
			return vs.ChannelID
		}
	}

	log.Printf("User %s is not in a voice channel", userID)
	return ""
}

func JoinVoiceChannel(discord *discordgo.Session, guildID, channelID string) error {
	vc, err := discord.ChannelVoiceJoin(guildID, channelID, false, true)
	if err != nil {
		return fmt.Errorf("failed to join voice channel: %w", err)
	}

	GlobalQueue.SaveVoiceConnection(guildID, vc)
	GlobalQueue.SetInVoiceChannel(guildID, true)

	return nil
}

func StartPlaybackIfNotActive(discord *discordgo.Session, guildID, textChannelID string) {
	if GlobalQueue.IsPlaying(guildID) {
		log.Printf("Already playing in guild %s, skipping duplicate call", guildID)
		return
	}

	peeked, ok := GlobalQueue.Peek(textChannelID)
	if !ok {
		log.Printf("Queue for channel %s is empty, nothing to play", textChannelID)
		GlobalQueue.SetInVoiceChannel(guildID, false)
		return
	}
	userID := peeked.RequestedBy

	if !GlobalQueue.IsInVoiceChannel(guildID) {
		voiceChannelID := findUserVoiceChannel(discord, guildID, userID)
		if voiceChannelID == "" {
			ErrorChan <- errors.New("unable to start playback: no users in voice channels")
			return
		}

		if err := JoinVoiceChannel(discord, guildID, voiceChannelID); err != nil {
			ErrorChan <- fmt.Errorf("failed to join voice channel: %w", err)
			return
		}
	} else {
		log.Printf("Already in a voice channel for guild %s", guildID)
	}

	vc, ok := GlobalQueue.GetVoiceConnection(guildID)
	if !ok || vc == nil {
		ErrorChan <- fmt.Errorf("no voice connection found for guild %s", guildID)
		return
	}

	current, ok := GlobalQueue.Pop(textChannelID)
	if !ok {
		log.Printf("Queue for channel %s is empty, nothing to play", textChannelID)
		GlobalQueue.SetInVoiceChannel(guildID, false)
		return
	}

	currentPath, found := GlobalQueue.GetDownloadedFile(current.Title)
	if !found {
		ErrorChan <- fmt.Errorf("next track '%s' not ready yet", current.Title)
		return
	}

	log.Printf("Starting playback of file %s in guild %s", currentPath, guildID)

	stop := make(chan bool)
	dgvoice.PlayAudioFile(vc, currentPath, stop)
	close(stop)

	log.Printf("Finished playing file %s in guild %s", currentPath, guildID)

	// if err := os.Remove(filePath); err != nil {
	// 	log.Printf("Failed to delete temp file %s: %v", filePath, err)
	// }

	next, ok := GlobalQueue.Peek(textChannelID)
	if !ok {
		log.Printf("No next track in queue for channel %s", textChannelID)
		return
	}

	log.Printf("Queuing next track: %s", next.Title)
	StartPlaybackIfNotActive(discord, guildID, textChannelID)
}
