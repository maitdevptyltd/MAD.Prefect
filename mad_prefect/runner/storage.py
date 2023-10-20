from prefect.runner.storage import GitRepository, GitCredentials


class ExtendedGitRepository(GitRepository):
    def __init__(
        self,
        url: str,
        credentials: GitCredentials | None = None,
        name: str | None = None,
        branch: str = "main",
        pull_interval: int | None = 60,
        post_pull_code_command: str = "poetry update",
    ):
        super().__init__(url, credentials, name, branch, pull_interval)
        self.post_pull_code_command = post_pull_code_command

    async def pull_code(self):
        parent = await super().pull_code()

        if self.post_pull_code_command:
            # import the destination's root direction incase it uses absolute imports
            import sys
            from anyio import run_process

            sys.path.append(str(self.destination))

            # run the command at the destination
            await run_process(self.post_pull_code_command, cwd=self.destination)

        return parent
