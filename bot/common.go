package bot

import (
	"log"

	"github.com/bwmarrin/discordgo"
)

func CheckNilErr(e error) {
	if e != nil {
		log.Fatalf("FATAL ERROR: %v", e)
	}
}

func GetUserID(i *discordgo.InteractionCreate) string {
	if i.Member != nil {
		return i.Member.User.ID
	}
	if i.User != nil {
		return i.User.ID
	}
	return ""
}
