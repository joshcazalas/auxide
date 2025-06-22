package bot

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os/exec"

	"github.com/bwmarrin/discordgo"
)

var voiceConn *discordgo.VoiceConnection

func findUserVoiceChannel(discord *discordgo.Session, guildID string, channelID string) string {
	queue := GlobalQueue.Get(channelID)

	requesterIDs := make(map[string]struct{})
	for _, video := range queue {
		if video.RequestedBy != "" {
			requesterIDs[video.RequestedBy] = struct{}{}
		}
	}

	guild, err := discord.State.Guild(guildID)
	if err != nil {
		return ""
	}

	for _, vs := range guild.VoiceStates {
		if _, found := requesterIDs[vs.UserID]; found {
			return vs.ChannelID
		}
	}

	return ""
}

func JoinVoiceChannel(discord *discordgo.Session, guildID string, channelID string) error {
	vc, err := discord.ChannelVoiceJoin(guildID, channelID, false, true)
	if err != nil {
		return fmt.Errorf("failed to join voice channel: %w", err)
	}

	SaveVoiceConnection(guildID, vc)
	SetInVoiceChannel(guildID, true)
	log.Printf("Joined voice channel %s in guild %s", channelID, guildID)

	return nil
}

func PlayAudio(discord *discordgo.Session, guildID string, voiceConn *discordgo.VoiceConnection, mp3FilePath string) error {
	SetPlaying(guildID, true)
	defer SetPlaying(guildID, false)

	cmd := exec.Command("ffmpeg",
		"-i", mp3FilePath,
		"-f", "opus",
		"-ar", "48000",
		"-ac", "2",
		"pipe:1",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get ffmpeg stdout: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start ffmpeg: %w", err)
	}

	voiceConn.Speaking(true)
	defer voiceConn.Speaking(false)

	reader := bufio.NewReader(stdout)
	buffer := make([]byte, 1920)

	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			voiceConn.OpusSend <- buffer[:n]
		}
		if err != nil {
			break
		}
	}

	if err := cmd.Wait(); err != nil {
		log.Printf("ffmpeg process exited with error: %v", err)
	}

	log.Printf("Finished playing audio in guild %s", guildID)
	return nil
}

func StartPlaybackIfNotActive(discord *discordgo.Session, guildID, channelID, filePath, userID string) {
	channelIDToJoin := findUserVoiceChannel(discord, guildID, channelID)
	if channelIDToJoin == "" {
		ErrorChan <- errors.New("❌ Unable to start playback: No users in any voice channels")
		return
	}

	if !IsInVoiceChannel(guildID) {
		if err := JoinVoiceChannel(discord, guildID, channelIDToJoin); err != nil {
			ErrorChan <- fmt.Errorf("failed to join voice channel: %w", err)
			return
		}
	} else {
		log.Printf("Bot already in a voice channel for guild %s", guildID)
	}

	if IsPlaying(guildID) {
		log.Printf("Audio is already playing in guild %s, skipping PlayAudio.", guildID)
		return
	}

	vc, ok := GetVoiceConnection(guildID)
	if !ok || vc == nil {
		ErrorChan <- fmt.Errorf("no voice connection found for guild %s", guildID)
		return
	}

	if err := PlayAudio(discord, guildID, vc, filePath); err != nil {
		ErrorChan <- fmt.Errorf("failed to play audio: %w", err)
		return
	}
}
