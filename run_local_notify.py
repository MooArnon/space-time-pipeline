from space_time_pipeline.notify.discord import DiscordNotify

# Replace this with your webhook URL
WEBHOOK_URL = "CHANGEME"

##############################################################################

if __name__ == "__main__":
    
    discord = DiscordNotify(webhook_url=WEBHOOK_URL)
    # discord.sent_message(message = "Test from Arnon's Macbook pro")
    discord.sent_message_image(
        message="Test image from Arnon's Macbook pro", 
        file_path="test.png",
    )

##############################################################################
