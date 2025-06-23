package bot

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/bwmarrin/discordgo"
)

// findUserVoiceChannel locates a voice channel based on the requesters in the queue.
func findUserVoiceChannel(discord *discordgo.Session, guildID, textChannelID string) string {
	queue := GlobalQueue.Get(textChannelID)

	requesterIDs := make(map[string]struct{})
	for _, video := range queue {
		if video.RequestedBy != "" {
			requesterIDs[video.RequestedBy] = struct{}{}
		}
	}

	guild, err := discord.State.Guild(guildID)
	if err != nil {
		log.Printf("Failed to fetch guild state: %v", err)
		return ""
	}

	for _, vs := range guild.VoiceStates {
		if _, found := requesterIDs[vs.UserID]; found {
			return vs.ChannelID
		}
	}

	return ""
}

// JoinVoiceChannel connects the bot to a voice channel and saves the connection.
func JoinVoiceChannel(discord *discordgo.Session, guildID, channelID string) error {
	vc, err := discord.ChannelVoiceJoin(guildID, channelID, false, true)
	if err != nil {
		return fmt.Errorf("failed to join voice channel: %w", err)
	}

	GlobalQueue.SaveVoiceConnection(guildID, vc)
	GlobalQueue.SetInVoiceChannel(guildID, true)
	log.Printf("🔊 Joined voice channel %s in guild %s", channelID, guildID)

	return nil
}

// PlayAudio streams an mp3 file using ffmpeg through Discord's Opus encoder.
func PlayAudio(discord *discordgo.Session, guildID string, vc *discordgo.VoiceConnection, filePath string) error {
	GlobalQueue.SetPlaying(guildID, true)
	defer GlobalQueue.SetPlaying(guildID, false)

	cmd := exec.Command("ffmpeg", "-i", filePath, "-f", "s16le", "-ar", "48000", "-ac", "2", "pipe:1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get ffmpeg stdout: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start ffmpeg: %w", err)
	}

	vc.Speaking(true)
	defer vc.Speaking(false)

	reader := bufio.NewReader(stdout)
	buffer := make([]byte, 1920)

	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			vc.OpusSend <- buffer[:n]
		}
		if err != nil {
			break
		}
	}

	if err := cmd.Wait(); err != nil {
		log.Printf("ffmpeg process exited with error: %v", err)
	}

	log.Printf("✅ Finished playing audio in guild %s", guildID)
	return nil
}

// StartPlaybackIfNotActive ensures the bot is connected and plays the given file.
func StartPlaybackIfNotActive(discord *discordgo.Session, guildID, textChannelID, filePath, userID string) {
	voiceChannelID := findUserVoiceChannel(discord, guildID, textChannelID)
	if voiceChannelID == "" {
		ErrorChan <- errors.New("❌ Unable to start playback: no users in voice channels")
		return
	}

	if !GlobalQueue.IsInVoiceChannel(guildID) {
		if err := JoinVoiceChannel(discord, guildID, voiceChannelID); err != nil {
			ErrorChan <- fmt.Errorf("failed to join voice channel: %w", err)
			return
		}
	} else {
		log.Printf("📡 Already in a voice channel for guild %s", guildID)
	}

	if GlobalQueue.IsPlaying(guildID) {
		log.Printf("⏩ Already playing in guild %s, skipping duplicate call", guildID)
		return
	}

	vc, ok := GlobalQueue.GetVoiceConnection(guildID)
	if !ok || vc == nil {
		ErrorChan <- fmt.Errorf("no voice connection found for guild %s", guildID)
		return
	}

	if err := PlayAudio(discord, guildID, vc, filePath); err != nil {
		ErrorChan <- fmt.Errorf("failed to play audio: %w", err)
		return
	}

	if err := os.Remove(filePath); err != nil {
		log.Printf("⚠️ Failed to delete temp file %s: %v", filePath, err)
	}

	next, ok := GlobalQueue.Pop(textChannelID)
	if !ok {
		log.Printf("📭 Queue for channel %s is now empty", textChannelID)
		return
	}

	nextPath, found := GlobalQueue.GetDownloadedFile(next.Title)
	if !found {
		ErrorChan <- fmt.Errorf("next track '%s' not ready yet", next.Title)
		return
	}

	go StartPlaybackIfNotActive(discord, guildID, textChannelID, nextPath, next.RequestedBy)
}
