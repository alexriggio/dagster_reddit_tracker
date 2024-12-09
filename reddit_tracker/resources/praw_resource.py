from dagster import EnvVar, ConfigurableResource
import praw

class PRAWResource(ConfigurableResource):
    client_id: str
    client_secret: str
    user_agent: str

    def get_client(self):
        return praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
        )
