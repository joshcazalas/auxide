package bot

import (
	"fmt"
	"strings"

	"github.com/bwmarrin/discordgo"
)

type SlashCommand struct {
	Command *discordgo.ApplicationCommand
	Handler func(s *discordgo.Session, i *discordgo.InteractionCreate)
}

var SlashCommands map[string]SlashCommand

func init() {
	SlashCommands = map[string]SlashCommand{
		"help": {
			Command: &discordgo.ApplicationCommand{
				Name:        "help",
				Description: "Get help information",
			},
			Handler: dynamicHelpHandler,
		},
		"bye": {
			Command: &discordgo.ApplicationCommand{
				Name:        "bye",
				Description: "Say goodbye to the bot",
			},
			Handler: func(s *discordgo.Session, i *discordgo.InteractionCreate) {
				s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
					Type: discordgo.InteractionResponseChannelMessageWithSource,
					Data: &discordgo.InteractionResponseData{
						Content: "Goodbye! See you later.",
					},
				})
			},
		},
		"ping": {
			Command: &discordgo.ApplicationCommand{
				Name:        "ping",
				Description: "Ping the bot",
			},
			Handler: func(s *discordgo.Session, i *discordgo.InteractionCreate) {
				s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
					Type: discordgo.InteractionResponseChannelMessageWithSource,
					Data: &discordgo.InteractionResponseData{
						Content: "pong",
					},
				})
			},
		},
		"play": {
			Command: &discordgo.ApplicationCommand{
				Name:        "play",
				Description: "Enter a song name for the bot to play",
				Options: []*discordgo.ApplicationCommandOption{
					{
						Type:        discordgo.ApplicationCommandOptionString,
						Name:        "query",
						Description: "Search term",
						Required:    true,
					},
				},
			},
			Handler: HandlePlayCommand,
		},
		"queue": {
			Command: &discordgo.ApplicationCommand{
				Name:        "queue",
				Description: "Get the current queue",
			},
			Handler: HandleGetQueueCommand,
		},
		"clear": {
			Command: &discordgo.ApplicationCommand{
				Name:        "clear",
				Description: "Clear the current queue",
			},
			Handler: HandleClearQueueCommand,
		},
		"skip": {
			Command: &discordgo.ApplicationCommand{
				Name:        "skip",
				Description: "Skip the current song",
			},
			Handler: HandleSkipCommand,
		},
		"stop": {
			Command: &discordgo.ApplicationCommand{
				Name:        "stop",
				Description: "Stop the current playback and clear the queue",
			},
			Handler: HandleStopCommand,
		},
		"pause": {
			Command: &discordgo.ApplicationCommand{
				Name:        "pause",
				Description: "Pause the current playback",
			},
			Handler: HandlePauseCommand,
		},
		"resume": {
			Command: &discordgo.ApplicationCommand{
				Name:        "resume",
				Description: "Resume the current playback",
			},
			Handler: HandleResumeCommand,
		},
	}
}

func RegisterSlashCommands(discord *discordgo.Session) {
	for _, guild := range discord.State.Guilds {
		for _, cmd := range SlashCommands {
			_, err := discord.ApplicationCommandCreate(discord.State.User.ID, guild.ID, cmd.Command)
			CheckNilErr(err)
		}
	}
}

func dynamicHelpHandler(s *discordgo.Session, i *discordgo.InteractionCreate) {
	var builder strings.Builder
	builder.WriteString("📖 **Available Commands:**\n\n")
	for _, cmd := range SlashCommands {
		builder.WriteString(fmt.Sprintf("`/%s` - %s\n", cmd.Command.Name, cmd.Command.Description))
	}

	s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: builder.String(),
		},
	})
}
