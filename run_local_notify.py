import os

from space_time_pipeline.notify import LineNotify

notifier = LineNotify(
    token = os.environ['LINE_TOKEN']
)

notifier.sent_message("test 555")
